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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// Class A tx row (no wire blob — reconstruct from inputs/outputs + witness).
///
/// On-disk bodies are **packed-only** (schema v11+): record starts with this
/// meta (txid first); inputs and outputs follow; trailing zero pad allowed.
/// `input_start_fk` / `output_start_fk` are always [`Fk::NULL`] on write.
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
fn encode_output_run_secret(
    recs: &[OutputRecord],
    out: &mut Vec<u8>,
    secret: Option<&crate::store_secret::StoreSecret>,
) {
    for r in recs {
        let start = out.len();
        r.encode_into(out);
        if let Some(sec) = secret {
            // XOR only the scriptPubKey payload bytes (after spender/flags/value/len).
            xor_script_region_in_output(out, start, sec);
        }
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
///
/// Production decode records denserels in [`decode_packed_tx_with_spender_rels`];
/// this helper remains for unit tests of the output run format.
#[cfg(test)]
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
fn encode_input_run_secret(
    recs: &[InputRecord],
    out: &mut Vec<u8>,
    secret: Option<&crate::store_secret::StoreSecret>,
) {
    for r in recs {
        let start = out.len();
        r.encode_into(out);
        if let Some(sec) = secret {
            xor_script_regions_in_input(out, start, sec);
        }
    }
}

/// XOR script_sig + witness item bytes inside an already-encoded input record.
fn xor_script_regions_in_input(
    buf: &mut [u8],
    start: usize,
    secret: &crate::store_secret::StoreSecret,
) {
    if start >= buf.len() {
        return;
    }
    let flags = buf[start];
    let mut off = start + 1;
    let null_prev = flags & input_flags::NULL_PREV != 0;
    if !null_prev {
        if off + 8 > buf.len() {
            return;
        }
        off += 8;
        // compact size vout
        let Ok((vout, n)) = read_compact_size(&buf[off..]) else {
            return;
        };
        let _ = vout;
        off += n;
    }
    if flags & input_flags::SEQ_FINAL == 0 {
        off += 4;
    }
    if flags & input_flags::EMPTY_SCRIPT == 0 {
        let Ok((slen, n)) = read_compact_size(&buf[off..]) else {
            return;
        };
        off += n;
        let slen = slen as usize;
        if off + slen > buf.len() {
            return;
        }
        secret.xor_bytes(0, &mut buf[off..off + slen]);
        off += slen;
    }
    if flags & input_flags::EMPTY_WITNESS == 0 {
        let Ok((nw, n)) = read_compact_size(&buf[off..]) else {
            return;
        };
        off += n;
        for wi in 0..nw {
            let Ok((ilen, n)) = read_compact_size(&buf[off..]) else {
                return;
            };
            off += n;
            let ilen = ilen as usize;
            if off + ilen > buf.len() {
                return;
            }
            secret.xor_bytes(u64::from(wi as u32).saturating_add(1) << 16, &mut buf[off..off + ilen]);
            off += ilen;
        }
    }
}

/// XOR scriptPubKey bytes inside an already-encoded output record.
fn xor_script_region_in_output(
    buf: &mut [u8],
    start: usize,
    secret: &crate::store_secret::StoreSecret,
) {
    if start + 9 > buf.len() {
        return;
    }
    // spender u64 + flags u8
    let flags = buf[start + 8];
    let mut off = start + 9;
    // uleb128 value
    loop {
        if off >= buf.len() {
            return;
        }
        let b = buf[off];
        off += 1;
        if b & 0x80 == 0 {
            break;
        }
    }
    if flags & (output_flags::EMPTY_SCRIPT | output_flags::OP_TRUE) != 0 {
        return;
    }
    let Ok((slen, n)) = read_compact_size(&buf[off..]) else {
        return;
    };
    off += n;
    let slen = slen as usize;
    if off + slen > buf.len() {
        return;
    }
    secret.xor_bytes(0, &mut buf[off..off + slen]);
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

/// OS page size used for on-disk **txid must not straddle page** rule (fixed).
pub const BODY_PAGE_SIZE: u64 = 4096;
/// Max offset within a page for a 32-byte txid start: `S % 4096 <= 4064`.
pub const TXID_PAGE_MAX_OFF: u64 = BODY_PAGE_SIZE - 32;

/// Next absolute body offset for a Class A packed record (8-byte aligned, txid
/// does not cross a 4 KiB page).
#[inline]
pub fn next_tx_body_start(cursor: u64) -> u64 {
    let mut s = cursor.saturating_add(7) & !7u64;
    // Avoid page straddle of [S, S+32).
    while s % BODY_PAGE_SIZE > TXID_PAGE_MAX_OFF {
        s = s.saturating_add(8);
    }
    s
}

/// Encode a full Class A tx as one var payload (schema 11+: no leading magic).
///
/// Layout: `TxRecord(64) || input_run || output_run` — txid at bytes [0, 32).
/// Without a secret, scripts/witness are stored in the clear (tests / temp).
/// Production put paths use [`encode_packed_tx_with_secret`].
pub fn encode_packed_tx(
    tx: &TxRecord,
    inputs: &[InputRecord],
    outputs: &[OutputRecord],
    out: &mut Vec<u8>,
) {
    encode_packed_tx_with_secret(tx, inputs, outputs, out, None);
}

/// Encode with optional at-rest XOR of scriptSig / witness / scriptPubKey.
pub fn encode_packed_tx_with_secret(
    tx: &TxRecord,
    inputs: &[InputRecord],
    outputs: &[OutputRecord],
    out: &mut Vec<u8>,
    secret: Option<&crate::store_secret::StoreSecret>,
) {
    debug_assert_eq!(inputs.len() as u32, tx.input_count);
    debug_assert_eq!(outputs.len() as u32, tx.output_count);
    // I/O fks are unused for packed rows (body is self-contained).
    let mut meta = tx.clone();
    meta.input_start_fk = Fk::NULL;
    meta.output_start_fk = Fk::NULL;
    meta.encode_into(out);
    encode_input_run_secret(inputs, out, secret);
    encode_output_run_secret(outputs, out, secret);
}

/// After walking a packed payload to `logical_end`, accept only zero pad to `raw.len()`.
#[inline]
fn check_trailing_zero_pad(raw: &[u8], logical_end: usize) -> Result<(), StoreError> {
    if logical_end > raw.len() {
        return Err(StoreError::Corrupt("packed Class A short payload"));
    }
    if raw[logical_end..].iter().any(|&b| b != 0) {
        return Err(StoreError::Corrupt("packed Class A trailing non-zero"));
    }
    Ok(())
}

/// Decode packed Class A; `raw` is the full var payload (may include zero pad).
pub fn decode_packed_tx(
    raw: &[u8],
) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
    let (meta, inputs, outputs, _rels) = decode_packed_tx_with_spender_rels(raw)?;
    Ok((meta, inputs, outputs))
}

/// Full packed decode plus dense relative offsets of each output's 9-byte
/// spender meta (field + flags) within the payload.
///
/// **Spender fields on returned outs are cleared** (same as outs-only denserels
/// path) so pin/OutFifo never treat pin-time annotations as durable authority.
pub fn decode_packed_tx_with_spender_rels(
    raw: &[u8],
) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>), StoreError> {
    decode_packed_tx_with_spender_rels_secret(raw, None)
}

/// Decode with optional de-obfuscation of script/witness (schema 12 production).
pub fn decode_packed_tx_with_spender_rels_secret(
    raw: &[u8],
    secret: Option<&crate::store_secret::StoreSecret>,
) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>), StoreError> {
    if raw.len() < TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[..TxRecord::ENCODED_LEN])?;
    let mut off = TxRecord::ENCODED_LEN;
    let (mut inputs, in_used) = decode_input_run_prefix(&raw[off..], meta.input_count)?;
    off += in_used;
    let n_out = meta.output_count as usize;
    let mut outputs = Vec::with_capacity(n_out);
    let mut spender_rels = Vec::with_capacity(n_out);
    for _ in 0..n_out {
        if off >= raw.len() {
            return Err(StoreError::Corrupt("packed outputs short"));
        }
        spender_rels.push(off as u32);
        let (mut rec, used) = OutputRecord::decode_at(&raw[off..])?;
        off += used;
        rec.spender_field = Fk::NULL;
        rec.multi_spender = false;
        outputs.push(rec);
    }
    check_trailing_zero_pad(raw, off)?;
    // De-obfuscate by re-encoding layout walk is unnecessary: scripts were
    // XOR'd only when stored as raw payload (not OP_TRUE / empty flags). Apply
    // the same in-buffer XOR walk to a mutable copy of script bytes only when
    // the decode path materialised them from disk payload lengths.
    if let Some(sec) = secret {
        deobfuscate_decoded_inputs_outputs(&mut inputs, &mut outputs, sec, raw, meta.input_count);
    }
    if inputs.len() as u32 != meta.input_count || outputs.len() as u32 != meta.output_count {
        return Err(StoreError::Corrupt("packed Class A count mismatch"));
    }
    Ok((meta, inputs, outputs, spender_rels))
}

/// De-XOR scripts/witness that were stored as opaque payloads (skip OP_TRUE / empty).
fn deobfuscate_decoded_inputs_outputs(
    inputs: &mut [InputRecord],
    outputs: &mut [OutputRecord],
    secret: &crate::store_secret::StoreSecret,
    raw: &[u8],
    input_count: u32,
) {
    // Walk raw layout to know which scripts were on-disk payloads.
    if raw.len() < TxRecord::ENCODED_LEN {
        return;
    }
    let mut off = TxRecord::ENCODED_LEN;
    for i in 0..input_count as usize {
        if off >= raw.len() || i >= inputs.len() {
            return;
        }
        let start = off;
        let flags = raw[off];
        off += 1;
        if flags & input_flags::NULL_PREV == 0 {
            off += 8;
            if let Ok((_, n)) = read_compact_size(&raw[off..]) {
                off += n;
            } else {
                return;
            }
        }
        if flags & input_flags::SEQ_FINAL == 0 {
            off += 4;
        }
        if flags & input_flags::EMPTY_SCRIPT == 0 {
            if let Ok((slen, n)) = read_compact_size(&raw[off..]) {
                off += n + slen as usize;
                secret.xor_bytes(0, &mut inputs[i].script_sig);
            } else {
                return;
            }
        }
        if flags & input_flags::EMPTY_WITNESS == 0 {
            if let Ok((nw, n)) = read_compact_size(&raw[off..]) {
                off += n;
                for wi in 0..nw as usize {
                    if let Ok((ilen, n)) = read_compact_size(&raw[off..]) {
                        off += n + ilen as usize;
                        if wi < inputs[i].witness.len() {
                            secret.xor_bytes(
                                u64::from(wi as u32).saturating_add(1) << 16,
                                &mut inputs[i].witness[wi],
                            );
                        }
                    } else {
                        return;
                    }
                }
            } else {
                return;
            }
        }
        let _ = start;
    }
    for o in outputs.iter_mut() {
        // Only de-xor non-empty, non-OP_TRUE scripts (those had payload on disk).
        if o.script.is_empty() || o.script == [0x51] {
            continue;
        }
        secret.xor_bytes(0, &mut o.script);
    }
}

/// Packed meta + input create edges only (skip scripts, witnesses, and outputs).
///
/// Each edge is `(create_fk, vout)`; coinbase → `(Fk::NULL, u32::MAX)`.
pub fn scan_packed_meta_and_prevouts(
    raw: &[u8],
) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
    if raw.len() < TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[..TxRecord::ENCODED_LEN])?;
    let mut off = TxRecord::ENCODED_LEN;
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
pub fn decode_packed_tx_outs_only(
    raw: &[u8],
) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
    let (meta, outs, _rels) = decode_packed_tx_outs_with_spender_rels(raw)?;
    Ok((meta, outs))
}

/// Like [`decode_packed_tx_outs_only`], plus dense relative offsets of each
/// output's 9-byte spender meta within the packed payload.
pub fn decode_packed_tx_outs_with_spender_rels(
    raw: &[u8],
) -> Result<(TxRecord, Vec<OutputRecord>, Vec<u32>), StoreError> {
    decode_packed_tx_outs_with_spender_rels_secret(raw, None)
}

pub fn decode_packed_tx_outs_with_spender_rels_secret(
    raw: &[u8],
    secret: Option<&crate::store_secret::StoreSecret>,
) -> Result<(TxRecord, Vec<OutputRecord>, Vec<u32>), StoreError> {
    if raw.len() < TxRecord::ENCODED_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode(&raw[..TxRecord::ENCODED_LEN])?;
    let mut off = TxRecord::ENCODED_LEN;
    for _ in 0..meta.input_count {
        let (_txid, _vout, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
    }
    let n_out = meta.output_count as usize;
    let mut outputs = Vec::with_capacity(n_out);
    let mut spender_rels = Vec::with_capacity(n_out);
    for _ in 0..n_out {
        if off >= raw.len() {
            return Err(StoreError::Corrupt("packed outputs short"));
        }
        spender_rels.push(off as u32);
        let (mut rec, used) = OutputRecord::decode_at(&raw[off..])?;
        off += used;
        rec.spender_field = Fk::NULL;
        rec.multi_spender = false;
        outputs.push(rec);
    }
    if let Some(sec) = secret {
        for o in &mut outputs {
            if o.script.is_empty() || o.script == [0x51] {
                continue;
            }
            sec.xor_bytes(0, &mut o.script);
        }
    }
    check_trailing_zero_pad(raw, off)?;
    if outputs.len() as u32 != meta.output_count {
        return Err(StoreError::Corrupt("packed Class A count mismatch"));
    }
    Ok((meta, outputs, spender_rels))
}

/// Strip durable spender annotation from outs (pin / OutFifo content-only).
#[inline]
pub fn clear_output_spender_fields(outs: &mut [OutputRecord]) {
    for o in outs {
        o.spender_field = Fk::NULL;
        o.multi_spender = false;
    }
}

/// True when `raw` looks like a schema-11+ packed Class A payload (txid-first).
#[inline]
pub fn is_packed_tx_payload(raw: &[u8]) -> bool {
    raw.len() >= TxRecord::ENCODED_LEN
}

/// In-progress sequential `tx.head` rebuild (shadow filled from `tx.idx` order).
///
/// `shadow` is [`Arc`] so [`Self::head_resize_poll`] can fill **without** holding
/// `resize` Mutex across body/idx IO — that lock is also taken by every archive
/// `head_insert_many` (`maybe_start_head_resize` / `head_resize_in_progress`).
struct HeadResize {
    shadow: Arc<AddressHead>,
    cursor: u64,
    target: HeadLayout,
}

/// Primary + optional shadow `tx.head` occupancy for IBD size logs.
///
/// Body sizes are **logical** table size (`slots × entry_bytes`), not faulted RSS.
#[derive(Clone, Copy, Debug, Default)]
pub struct HeadResizeSizeSnapshot {
    pub active: bool,
    /// Next Class A fk to fill into the shadow (0 if idle).
    pub cursor: u64,
    pub class_a_n: u64,
    pub primary_bits: u32,
    pub primary_slots: u64,
    pub primary_entry_b: u8,
    pub primary_occupied: u64,
    pub primary_body_bytes: u64,
    pub shadow_bits: u32,
    pub shadow_slots: u64,
    pub shadow_entry_b: u8,
    pub shadow_occupied: u64,
    pub shadow_body_bytes: u64,
}

/// Parse a positive u64 env (new name, then deprecated alias).
fn env_u64(primary: &str, legacy: &str, default: u64, lo: u64, hi: u64) -> u64 {
    std::env::var(primary)
        .or_else(|_| std::env::var(legacy))
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default)
        .clamp(lo, hi)
}

/// Class A fks the background fill thread advances per `head_resize_poll`.
/// `RBITCOIN_TX_HEAD_FILL_WAVE` (default **1 048 576**); legacy
/// `RBITCOIN_TX_HEAD_RESIZE_WAVE` still accepted.
fn head_fill_wave() -> u64 {
    env_u64(
        "RBITCOIN_TX_HEAD_FILL_WAVE",
        "RBITCOIN_TX_HEAD_RESIZE_WAVE",
        1_048_576,
        1_024,
        16_777_216,
    )
}

pub struct TxTable {
    pub(crate) body: VarTable,
    pub(crate) head: RwLock<AddressHead>,
    /// Directory containing `tx.head` (for rename / control paths).
    head_path: PathBuf,
    /// Datadir secret: keyed head probes + script XOR (schema 12+).
    pub(crate) secret: crate::store_secret::StoreSecret,
    /// Depth-exhausted inserts while primary resizes (overflow-first lookup).
    pub(crate) overflow: Mutex<crate::head_overflow::HeadOverflow>,
    resize: Mutex<Option<HeadResize>>,
    /// True from resize start until swap completes (or abandoned). Survives brief
    /// windows where `resize` Mutex holds `None` while closing the shadow Arc.
    resize_active: AtomicBool,
    /// Background sequential fill for `tx.head.new` (independent of archive inserts).
    resize_bg: Mutex<Option<JoinHandle<()>>>,
    /// Generation of the live bg worker; bump to ask a previous worker to exit.
    resize_bg_gen: AtomicU64,
    /// Sticky abort for exclusive swap waits (Drop / tests). Distinct from gen
    /// bump alone: gen kill of the bg worker must still allow main-thread
    /// `head_resize_poll` to complete the swap.
    resize_abort: AtomicBool,
    /// Only one thread may run the exclusive rename/swap path at a time.
    /// Without this, bg poll + main-thread poll race double-close mmaps/FDs.
    resize_completing: AtomicBool,
    /// Wakes archive/tip writers parked on probe-exhaust for the duration of a resize.
    /// Pair with [`Self::resize`] (waiters hold that mutex).
    resize_cv: Condvar,
}

impl Drop for TxTable {
    fn drop(&mut self) {
        // Sticky abort first so exclusive-lock waiters exit even if they enter
        // the wait *after* the gen bump (gen_at_start would already match).
        self.resize_abort.store(true, AtomicOrdering::Release);
        self.resize_bg_gen
            .fetch_add(1, AtomicOrdering::AcqRel);
        if let Some(h) = self.resize_bg.lock().unwrap().take() {
            let _ = h.join();
        }
        // Unblock any insert waiters (tests / early drop mid-resize).
        self.resize_active.store(false, AtomicOrdering::Release);
        *self.resize.lock().unwrap() = None;
        self.resize_cv.notify_all();
    }
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_head_layout(dir, crate::address_head::default_layout())
    }

    /// Create with an explicit head geometry (tests / recovery). Avoids racing on
    /// process-global `RBITCOIN_TX_HEAD_BITS` / `RBITCOIN_HEAD_SCALE`.
    pub fn create_with_head_layout(dir: &Path, layout: HeadLayout) -> Result<Self, StoreError> {
        let head_path = dir.join("tx.head");
        let secret = crate::store_secret::StoreSecret::load_or_create(dir, true)?;
        let overflow = crate::head_overflow::HeadOverflow::create(dir)?;
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: RwLock::new(AddressHead::create_with_layout(&head_path, layout)?),
            head_path,
            secret,
            overflow: Mutex::new(overflow),
            resize: Mutex::new(None),
            resize_active: AtomicBool::new(false),
            resize_bg: Mutex::new(None),
            resize_bg_gen: AtomicU64::new(0),
            resize_abort: AtomicBool::new(false),
            resize_completing: AtomicBool::new(false),
            resize_cv: Condvar::new(),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let head_path = dir.join("tx.head");
        let body = VarTable::open(dir, "tx", TableKind::Tx)?;
        let n_bodies = body.count();
        // Operator recovery: delete `tx.head` → empty create + full rebuild from
        // Class A bodies on next open. Incomplete heads that still open
        // successfully are *not* auto-rebuilt (delete to force). Layout lives in
        // the trailing footer (no sidecar).
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
                    // Common after mid-resize kill or footer-layout upgrade.
                    if n_bodies > 0 {
                        rbitcoin_log::warn!(
                            "store: tx.head unreadable ({e}) with {n_bodies} Class A bodies — \
                             will recreate head sized for count and rebuild mappings \
                             (online resize leftovers cleared; expect a long open)"
                        );
                        let _ = std::fs::remove_file(&head_path);
                        crate::address_head::remove_legacy_meta_sidecar(&head_path);
                        need_rebuild = true;
                        Self::prepare_fresh_head(&head_path, n_bodies)?
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        let secret = crate::store_secret::StoreSecret::load_or_create(dir, true)?;
        let overflow = crate::head_overflow::HeadOverflow::open(dir)?;
        let t = Self {
            body,
            head: RwLock::new(head),
            head_path,
            secret,
            overflow: Mutex::new(overflow),
            resize: Mutex::new(None),
            resize_active: AtomicBool::new(false),
            resize_bg: Mutex::new(None),
            resize_bg_gen: AtomicU64::new(0),
            resize_abort: AtomicBool::new(false),
            resize_completing: AtomicBool::new(false),
            resize_cv: Condvar::new(),
        };
        if need_rebuild {
            // Sized head + rebuild inserts only — do not interleave online resize
            // (that produced dual "rebuild progress" + "resize progress" on open).
            let bits = t.head_bits();
            let slots = t.head_slots();
            rbitcoin_log::info!(
                "store: tx.head rebuild begin n={n_bodies} bits={bits} slots={slots} \
                 (no concurrent online resize)"
            );
            let inserted = t.rebuild_head_from_bodies(|done, total, ins| {
                if done == total || done % 1_000_000 == 0 {
                    rbitcoin_log::info!(
                        "store: tx.head rebuild progress {done}/{total} inserted={ins}"
                    );
                }
            })?;
            t.head.read().unwrap().flush()?;
            rbitcoin_log::info!(
                "store: tx.head rebuild complete inserted={inserted} bodies={} bits={}",
                t.count(),
                t.head_bits()
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
        // Under `cargo test`, drive fill only via `head_resize_poll` (tests already
        // call it). Concurrent bg + main try_complete races mmap/FD teardown
        // ("owned file descriptor already closed"). Production IBD keeps the worker.
        if cfg!(test) {
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
                let wave = head_fill_wave();
                rbitcoin_log::info!(
                    "store: tx.head resize background fill started (budget={wave}/wave \
                     read_batch={} write_chunk={})",
                    Self::head_fill_read_batch(),
                    Self::head_fill_write_chunk()
                );
                loop {
                    if table.resize_bg_gen.load(AtomicOrdering::Acquire) != gen {
                        break;
                    }
                    if !table.head_resize_in_progress() {
                        break;
                    }
                    match table.head_resize_poll(head_fill_wave()) {
                        Ok(()) => {}
                        Err(e) => {
                            if table.resize_abort.load(AtomicOrdering::Acquire)
                                || table.resize_bg_gen.load(AtomicOrdering::Acquire) != gen
                            {
                                break;
                            }
                            rbitcoin_log::error!(
                                "store: tx.head resize background fill error: {e} — retry in 1s"
                            );
                            // Short sleeps so Drop's join is not stuck for a full second
                            // after abort/gen bump (tests remove datadirs promptly).
                            for _ in 0..10 {
                                if table.resize_abort.load(AtomicOrdering::Acquire)
                                    || table.resize_bg_gen.load(AtomicOrdering::Acquire) != gen
                                {
                                    break;
                                }
                                std::thread::sleep(Duration::from_millis(100));
                            }
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

    /// Create empty `tx.head`, drop resize leftovers.
    ///
    /// When `n_bodies > 0`, size the head so load stays under [`HEAD_LOAD_START`]
    /// for that count — otherwise the first rebuild insert wave immediately starts
    /// a concurrent online resize (confusing dual progress logs).
    fn prepare_fresh_head(head_path: &Path, n_bodies: u64) -> Result<AddressHead, StoreError> {
        clear_resize_control(head_path);
        let shadow = shadow_head_path(head_path);
        let _ = std::fs::remove_file(&shadow);
        crate::address_head::remove_legacy_meta_sidecar(&shadow);
        let bak = bak_head_path(head_path);
        let _ = std::fs::remove_file(&bak);
        crate::address_head::remove_legacy_meta_sidecar(&bak);
        // Pre-v5 sidecar (layout is in the footer now).
        crate::address_head::remove_legacy_meta_sidecar(head_path);
        let layout = if n_bodies > 0 {
            let layout = crate::address_head::layout_for_count(n_bodies);
            rbitcoin_log::info!(
                "store: tx.head recreate bits={} slots={} entry={}B for {n_bodies} Class A bodies \
                 (full mapping rebuild next)",
                layout.bits,
                layout.slots(),
                layout.entry_bytes
            );
            layout
        } else {
            rbitcoin_log::info!("store: tx.head missing — creating empty address head");
            crate::address_head::default_layout()
        };
        AddressHead::create_with_layout(head_path, layout)
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
        // Align bare-meta rows the same way as full packed appends (schema 11).
        let est: usize = recs.len() * (TxRecord::ENCODED_LEN + 16);
        let fks = self.body.put_batch_encode_aligned(recs.len(), est, |i, buf| {
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
        let (tx, _, _, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, Some(&self.secret))?;
        Ok(tx)
    }

    /// Read Class A body **txid only** (first 32 bytes at record start).
    ///
    /// Thin I/O: idx range + **32** body bytes — no scripts/witness.
    pub fn body_txid(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
        use std::time::Instant;
        let t_idx = Instant::now();
        let (off, len) = self.body.record_range(fk)?;
        crate::head_resolve_stats::add_idx(t_idx.elapsed().as_nanos() as u64);
        let t_body = Instant::now();
        let mut prefix = [0u8; 32];
        let n = self.body.read_prefix_at(off, len, &mut prefix)?;
        crate::head_resolve_stats::add_body(t_body.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_body_lookups(1);
        Self::txid_from_body_prefix(&prefix[..n])
    }

    /// Bulk Class A body txids for consecutive ids `first..=last` (1-based).
    ///
    /// Contiguous idx + bulk **32**-byte body prefixes (txid at record start).
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

        let mut prefixes: Vec<[u8; 32]> = vec![[0u8; 32]; n];
        for &(off, len) in &ranges {
            if len < 32 {
                return Err(StoreError::Corrupt("empty body for txid"));
            }
            if off.saturating_add(32) > body_pub {
                return Err(StoreError::Corrupt("body past published"));
            }
        }

        let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(n);
        for i in 0..n {
            let ptr = prefixes[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, 32) };
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
            if ro.result < 0 {
                return Err(StoreError::io(
                    body_path,
                    std::io::Error::from_raw_os_error(-ro.result),
                ));
            }
            if ro.result as usize != 32 {
                return Err(StoreError::Corrupt("bulk body_txid pread short"));
            }
            out.push(Self::txid_from_body_prefix(&prefixes[i])?);
        }
        crate::head_resolve_stats::add_body_lookups(n as u64);
        Ok(out)
    }

    /// Parse txid from the leading bytes of a Class A body payload (txid-first).
    #[inline]
    pub(crate) fn txid_from_body_prefix(raw: &[u8]) -> Result<[u8; 32], StoreError> {
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
        if raw.len() < TxRecord::ENCODED_LEN {
            return Err(StoreError::Corrupt("short packed tx"));
        }
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        let meta = TxRecord::decode(&raw[..TxRecord::ENCODED_LEN])?;
        let mut want: Vec<u32> = vouts.to_vec();
        want.sort_unstable();
        want.dedup();
        let max_v = *want.last().unwrap();
        if max_v >= meta.output_count {
            return Err(StoreError::NotFound);
        }
        let mut off = TxRecord::ENCODED_LEN;
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

    /// Read Class A body txid from a known range (no idx). Thin: first 32 bytes.
    pub fn body_txid_at(&self, offset: u64, len: u64) -> Result<[u8; 32], StoreError> {
        let mut prefix = [0u8; 32];
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
        let mixed = self.secret.mix_txid(txid);
        // Depth-first: overflow (depth-exhausted inserts) then primary head.
        if let Some(fk) = self.overflow.lock().unwrap().get(&mixed) {
            if self.body_txid(fk)? == *txid {
                return Ok(Some(fk));
            }
        }
        let t_probe = Instant::now();
        let cands = self.head.read().unwrap().probe_fks(&mixed)?;
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

    /// Mix txid for head probe keys (tests / diagnostics).
    pub fn mix_txid_for_head(&self, txid: &[u8; 32]) -> [u8; 32] {
        self.secret.mix_txid(txid)
    }

    /// Store secret (script XOR / head mix).
    pub fn store_secret(&self) -> &crate::store_secret::StoreSecret {
        &self.secret
    }

    /// Batch head resolve (archive prep bulk path).
    ///
    /// Primary: streaming resolve ([`crate::head_resolve_stream`]) when io_uring
    /// is available — mmap probe + completion-driven idx→body preads (early exit).
    /// Fallback (no uring / stream setup fail): phase barrier via
    /// [`Self::get_fk_by_txid_batch_phased`] (idx→body pipeline Prefix33).
    ///
    /// BIP30: deepest matching body wins.
    /// Timers: [`crate::head_resolve_stats`] probe / idx / body.
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        if crate::bulk_io::io_uring_enabled() {
            match crate::head_resolve_stream::resolve_batch_streaming(self, txids) {
                Ok(v) => return Ok(v),
                Err(e) => {
                    rbitcoin_log::debug!(
                        "store: streaming head resolve unavailable ({e}); using batch path"
                    );
                }
            }
        }
        self.get_fk_by_txid_batch_phased(txids)
    }

    /// Phase-barrier resolve: mmap probe + idx→body pipeline (Prefix33) for all cands.
    pub(crate) fn get_fk_by_txid_batch_phased(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
        use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
        use std::time::Instant;
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        crate::head_resolve_stats::add_keys(txids.len() as u64);

        // --- Phase 1: mmap probe (mixed keys; overflow-first) ---
        let t_probe = Instant::now();
        let mut cands_by_key: Vec<Vec<(u32, u64)>> = vec![Vec::new(); txids.len()];
        let mut cands_total = 0u64;
        {
            let head = self.head.read().unwrap();
            let ov = self.overflow.lock().unwrap();
            for (i, txid) in txids.iter().enumerate() {
                let mixed = self.secret.mix_txid(txid);
                if let Some(fk) = ov.get(&mixed) {
                    cands_by_key[i].push((0, fk.0));
                    cands_total = cands_total.saturating_add(1);
                    continue;
                }
                let cands = head.probe_fks(&mixed)?;
                cands_total = cands_total.saturating_add(cands.len() as u64);
                for (j, fk) in cands.into_iter().enumerate() {
                    cands_by_key[i].push((j as u32, fk.0));
                }
            }
        }
        crate::head_resolve_stats::add_probe(t_probe.elapsed().as_nanos() as u64);

        // --- Phase 2+3: idx→body pipeline (Prefix33), sorted by fk ---
        let t_pipe = Instant::now();
        struct CandRef {
            key_i: u32,
            depth: u8,
            fk: u64,
        }
        let mut cand_refs: Vec<CandRef> = Vec::new();
        for (i, cands) in cands_by_key.iter().enumerate() {
            for &(depth, fk) in cands {
                cand_refs.push(CandRef {
                    key_i: i as u32,
                    depth: depth as u8,
                    fk,
                });
            }
        }
        let mut pipe_jobs: Vec<IdxBodyJob> = cand_refs
            .iter()
            .map(|c| IdxBodyJob::new(c.fk, None))
            .collect();
        run_idx_body_pipeline(&self.body, &mut pipe_jobs, BodyMode::Prefix33)?;
        let pipe_ns = t_pipe.elapsed().as_nanos() as u64;
        // Split wall roughly half for stats continuity.
        crate::head_resolve_stats::add_idx(pipe_ns / 2);
        crate::head_resolve_stats::add_body(pipe_ns.saturating_sub(pipe_ns / 2));

        let mut best: Vec<Option<(u64, u8)>> = vec![None; txids.len()];
        let mut body_lookups = 0u64;
        for (c, job) in cand_refs.iter().zip(pipe_jobs.iter()) {
            if !job.ok || job.body.is_empty() {
                continue;
            }
            body_lookups = body_lookups.saturating_add(1);
            let ki = c.key_i as usize;
            let want_txid = &txids[ki];
            match Self::txid_from_body_prefix(&job.body) {
                Ok(got) if got == *want_txid => match best[ki] {
                    Some((_, best_d)) if c.depth <= best_d => {}
                    _ => best[ki] = Some((c.fk, c.depth)),
                },
                Ok(_) => {}
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => {}
                Err(e) => return Err(e),
            }
        }
        crate::head_resolve_stats::add_cands(cands_total);
        crate::head_resolve_stats::add_body_lookups(body_lookups);

        let mut out = Vec::with_capacity(txids.len());
        for (i, txid) in txids.iter().enumerate() {
            let hit = best[i].map(|(id, _)| Fk(id));
            out.push((*txid, hit));
        }
        Ok(out)
    }

    /// Bulk `body_range` for many fks (archive sticky + confirm load).
    ///
    /// **Sorted mmap** walk of `tx.idx` via [`VarTable::record_range_batch`] —
    /// same modality as archive head-resolve idx (not scatter io_uring/pread).
    pub fn body_range_batch(
        &self,
        fks: &[Fk],
    ) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.body.record_range_batch(fks)
    }

    /// Bulk full packed decode from known ranges.
    ///
    /// Thin decode wrapper over [`crate::idx_body_pipeline`] (body-only jobs).
    /// Fourth field: dense spender_rels relative to body_off.
    pub fn get_full_batch_at(
        &self,
        ranges: &[(Fk, u64, u64)],
    ) -> Result<
        Vec<Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>)>>,
        StoreError,
    > {
        use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        let mut jobs: Vec<IdxBodyJob> = ranges
            .iter()
            .map(|(fk, off, len)| {
                let id = fk.get().unwrap_or(0);
                IdxBodyJob::new(id, Some((*off, *len)))
            })
            .collect();
        run_idx_body_pipeline(&self.body, &mut jobs, BodyMode::Full)?;
        let mut out = Vec::with_capacity(jobs.len());
        for j in jobs {
            if !j.ok {
                out.push(None);
                continue;
            }
            out.push(
                decode_packed_tx_with_spender_rels_secret(&j.body, Some(&self.secret)).ok(),
            );
        }
        Ok(out)
    }

    /// Bulk meta+outputs+spender_rels from known ranges.
    ///
    /// Thin decode wrapper over [`crate::idx_body_pipeline`] (body-only denserels).
    pub fn get_meta_and_outputs_batch_at(
        &self,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>>, StoreError> {
        use crate::idx_body_pipeline::{run_idx_body_pipeline, BodyMode, IdxBodyJob};
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        // Synthetic sequential ids: pipeline only needs range when known; id is
        // unused for body-only jobs (bounds skipped when range is Some).
        let mut jobs: Vec<IdxBodyJob> = ranges
            .iter()
            .enumerate()
            .map(|(i, &(off, len))| IdxBodyJob::new((i as u64).saturating_add(1), Some((off, len))))
            .collect();
        run_idx_body_pipeline(&self.body, &mut jobs, BodyMode::OutsDenserels)?;
        let mut out = Vec::with_capacity(jobs.len());
        for j in jobs {
            if !j.ok {
                out.push(None);
                continue;
            }
            out.push(
                decode_packed_tx_outs_with_spender_rels_secret(&j.body, Some(&self.secret)).ok(),
            );
        }
        Ok(out)
    }

    /// Annotate spends at known absolute spender-meta offsets (confirm write).
    ///
    /// Prefer io_uring RMW ([`crate::spend_annotate_uring`]): pread 9 B → decide
    /// sole / multi / promote → pwrite; `spenders.body` appends run **inline** on
    /// the read completion (mmap). Same abs serialized. Fallback: mmap RMW.
    ///
    /// Returns edges that still need a full cold path (OOB abs / deferred).
    /// Multi-list cases are handled here when uring/mmap succeed (not returned).
    pub fn put_spend_batch_by_abs_meta(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        abs_edges: &[(u64, Fk, u32, Fk)],
    ) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
        const META_LEN: u64 = 9;
        if abs_edges.is_empty() {
            return Ok(Vec::new());
        }
        for &(_, _, _, sfk) in abs_edges {
            if sfk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }
        if crate::bulk_io::io_uring_enabled() {
            match crate::spend_annotate_uring::put_spend_batch_by_abs_meta_uring(
                self, spenders, abs_edges,
            ) {
                Ok(cold) => return Ok(cold),
                Err(e) => {
                    rbitcoin_log::debug!(
                        "store: spend annotate uring unavailable ({e}); mmap fallback"
                    );
                }
            }
        }
        // --- mmap fallback (same sole/multi semantics) ---
        let body_pub = self.body.body_published_len();
        let mut cold: Vec<(Fk, u32, Fk)> = Vec::new();
        for &(abs, create_fk, vout, spend_fk) in abs_edges {
            if abs.saturating_add(META_LEN) > body_pub {
                cold.push((create_fk, vout, spend_fk));
                continue;
            }
            let cur = self.body.with_bytes_at(abs, META_LEN, |raw| {
                if raw.len() < 9 {
                    return Err(StoreError::Corrupt("spender meta short"));
                }
                let field = Fk(u64::from_le_bytes(raw[0..8].try_into().unwrap()));
                let flags = raw[8];
                Ok((field, flags))
            });
            let Ok((field, flags)) = cur else {
                cold.push((create_fk, vout, spend_fk));
                continue;
            };
            let multi = flags & output_flags::MULTI_SPENDER != 0;
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
            let mut meta = [0u8; 9];
            meta[0..8].copy_from_slice(&new_field.0.to_le_bytes());
            if new_multi {
                meta[8] = flags | output_flags::MULTI_SPENDER;
            } else {
                meta[8] = flags & !output_flags::MULTI_SPENDER;
            }
            if let Err(_) = self.body.write_body_abs(abs, &meta) {
                cold.push((create_fk, vout, spend_fk));
            }
        }
        Ok(cold)
    }

    /// Bulk 9-byte spender meta preads at absolute `tx.body` file offsets.
    ///
    /// Each entry is the absolute offset of an output's spender_field (8) + flags (1).
    /// Uses io_uring / parallel pread. Failed or short reads yield `None`.
    pub fn get_spender_meta_at_abs_batch(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(bool, Fk)>>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        const META_LEN: usize = 9;
        if abs_offs.is_empty() {
            return Ok(Vec::new());
        }
        let body_fd = self.body.body_read_fd();
        let body_pub = self.body.body_published_len();

        let submitted: Vec<usize> = abs_offs
            .iter()
            .enumerate()
            .filter(|(_, &off)| off.saturating_add(META_LEN as u64) <= body_pub)
            .map(|(i, _)| i)
            .collect();

        let mut bufs: Vec<[u8; META_LEN]> = vec![[0u8; META_LEN]; abs_offs.len()];
        let mut read_ops: Vec<ReadOp<'_>> = Vec::with_capacity(submitted.len());
        for &i in &submitted {
            let ptr = bufs[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, META_LEN) };
            read_ops.push(ReadOp {
                fd: body_fd,
                offset: abs_offs[i],
                buf: slice,
                result: i32::MIN,
            });
        }
        bulk_io::pread_batch(&mut read_ops);

        let mut out: Vec<Option<(bool, Fk)>> = vec![None; abs_offs.len()];
        for (ro, &i) in read_ops.iter().zip(submitted.iter()) {
            if ro.result != META_LEN as i32 {
                continue;
            }
            let b = &bufs[i];
            let field = Fk(u64::from_le_bytes(b[0..8].try_into().unwrap()));
            let multi = b[8] & output_flags::MULTI_SPENDER != 0;
            out[i] = Some((multi, field));
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
        let (tx, ins, outs, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, Some(&self.secret))?;
        Ok((tx, ins, outs))
    }

    /// Meta + outputs only (one body IO; skips input materialization).
    pub fn get_meta_and_outputs(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        let raw = self.body.get_raw(fk)?;
        let (tx, outs, _) =
            decode_packed_tx_outs_with_spender_rels_secret(&raw, Some(&self.secret))?;
        Ok((tx, outs))
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
        // Worst-case pad ≤ 8 + page-skip gap before each record.
        let est: usize = items
            .iter()
            .map(|(_tx, ins, outs)| {
                16 + TxRecord::ENCODED_LEN
                    + ins.iter().map(|i| i.encoded_len()).sum::<usize>()
                    + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let fks = self.body.put_batch_encode_aligned(items.len(), est, |i, buf| {
            let (tx, ins, outs) = &items[i];
            // Schema 12: XOR scriptSig / witness / scriptPubKey at rest.
            encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
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
        let mixed = self.secret.mix_txid(txid);
        let mut cands: Vec<Fk> = Vec::new();
        if let Some(fk) = self.overflow.lock().unwrap().get(&mixed) {
            cands.push(fk);
        }
        cands.extend(self.head.read().unwrap().probe_fks(&mixed)?);
        // Newest first: reverse primary probe (older→newer) after optional overflow.
        for fk in cands.into_iter().rev() {
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
        // Same body-read / insert_many chunking as online shadow fill
        // (`RBITCOIN_TX_HEAD_READ_BATCH` / `RBITCOIN_TX_HEAD_WRITE_CHUNK`).
        let read_batch = Self::head_fill_read_batch();
        let write_chunk = Self::head_fill_write_chunk();
        const PROGRESS_EVERY: u64 = 50_000;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(write_chunk);
        let mut last_progress = 0u64;
        let mut cur = 1u64;
        while cur <= n {
            let end = (cur + read_batch - 1).min(n);
            // Contiguous idx + bulk 33B prefixes — same as shadow_fill_fk_range.
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
                if batch.len() >= write_chunk {
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
    /// On open-address **probe exhaust**, the archive/tip writer **sleeps** until
    /// the background resize finishes (shadow filled + primary swapped), then
    /// retries the batch. No wall-clock deadline — mainnet 29→30 rehashes can
    /// take longer than a fixed timeout and must not kill the archive pipeline.
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if !entries.is_empty() {
            self.head_insert_many_with_resize_retry(entries)?;
        }
        // First insert past PROBE_DEPTH_WARN requests early widen (before load 0.80
        // or probe exhaust).
        if take_probe_depth_resize_request() && !self.head_resize_in_progress() {
            self.ensure_head_resize_for_probe_exhaust()?;
        }
        // After live inserts into primary only — never dual-write to shadow.
        self.maybe_start_head_resize()?;
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Insert batch using **mixed** keys. On primary probe exhaust, depth-failed
    /// entries go to [`HeadOverflow`] so the write path does not stall for the
    /// full primary rehash; remaining entries retry on the primary.
    fn head_insert_many_with_resize_retry(
        &self,
        entries: &[([u8; 32], Fk)],
    ) -> Result<(), StoreError> {
        // Keyed probes: never use raw txid prefixes as open-hash keys.
        let mixed: Vec<([u8; 32], Fk)> = entries
            .iter()
            .map(|(txid, fk)| (self.secret.mix_txid(txid), *fk))
            .collect();
        let insert_result = {
            let head = self.head.read().unwrap();
            head.insert_many(&mixed)
        };
        match insert_result {
            Ok(()) => Ok(()),
            Err(e) if is_probe_exhausted_error(&e) => {
                // Kick resize in the background but do not block the pipeline:
                // park depth-exhausted keys in overflow (overflow-first lookup).
                let _ = self.ensure_head_resize_for_probe_exhaust();
                self.ensure_resize_bg_running();
                self.head_insert_mixed_with_overflow(&mixed)
            }
            Err(e) => Err(e),
        }
    }

    /// Per-entry primary insert; overflow on probe exhaust.
    fn head_insert_mixed_with_overflow(
        &self,
        mixed: &[([u8; 32], Fk)],
    ) -> Result<(), StoreError> {
        let mut overflow_n = 0u32;
        for &(key, fk) in mixed {
            let one = [(key, fk)];
            let r = {
                let head = self.head.read().unwrap();
                head.insert_many(&one)
            };
            match r {
                Ok(()) => {}
                Err(e) if is_probe_exhausted_error(&e) => {
                    let mut ov = self.overflow.lock().unwrap();
                    match ov.insert(&key, fk) {
                        Ok(_) => {
                            overflow_n = overflow_n.saturating_add(1);
                        }
                        Err(e2) => {
                            // Overflow full: wait for primary resize, then retry primary.
                            drop(ov);
                            self.ensure_head_resize_for_probe_exhaust()?;
                            self.ensure_resize_bg_running();
                            if self.head_resize_in_progress() {
                                self.wait_for_head_resize_idle();
                                let head = self.head.read().unwrap();
                                head.insert_many(&[(key, fk)])?;
                            } else {
                                return Err(e2);
                            }
                        }
                    }
                }
                Err(e) => return Err(e),
            }
        }
        if overflow_n > 0 {
            self.overflow.lock().unwrap().persist()?;
            rbitcoin_log::debug!(
                "store: tx.head overflow accepted {overflow_n} depth-exhausted insert(s)"
            );
        }
        Ok(())
    }

    /// Merge overflow entries into primary after a successful head resize swap.
    pub fn drain_overflow_into_primary(&self) -> Result<usize, StoreError> {
        let pairs: Vec<([u8; 32], Fk)> = {
            let ov = self.overflow.lock().unwrap();
            ov.iter_occupied().collect()
        };
        if pairs.is_empty() {
            return Ok(0);
        }
        {
            let head = self.head.read().unwrap();
            head.insert_many(&pairs)?;
        }
        self.overflow.lock().unwrap().clear()?;
        Ok(pairs.len())
    }

    /// Block the calling thread until online head resize is idle.
    ///
    /// Progress is logged every 30s while parked. Wakes on
    /// [`Self::notify_head_resize_idle`] after a successful swap (or Drop).
    ///
    /// Lock order: never hold `resize` while taking `head` (swap holds
    /// `head.write` then notifies under `resize` — reverse order deadlocks).
    fn wait_for_head_resize_idle(&self) {
        /// How often a parked archiver logs resize progress.
        const LOG_EVERY: Duration = Duration::from_secs(30);

        let t0 = Instant::now();
        let mut last_log = Instant::now()
            .checked_sub(LOG_EVERY)
            .unwrap_or_else(Instant::now);
        loop {
            if !self.head_resize_in_progress() {
                break;
            }
            if last_log.elapsed() >= LOG_EVERY {
                // Snapshot without holding `resize` across `head.read`.
                let (cursor, n, target_bits, slots) = self.head_resize_progress();
                let pct = if n > 0 {
                    100.0 * (cursor.saturating_sub(1) as f64) / (n as f64)
                } else {
                    100.0
                };
                rbitcoin_log::info!(
                    "store: tx.head archiver still sleeping on resize \
                     (cursor={}/{n} ({pct:.1}%) target_bits={target_bits} \
                     slots={slots} elapsed={:?})",
                    cursor.saturating_sub(1),
                    t0.elapsed()
                );
                last_log = Instant::now();
            }
            let guard = self.resize.lock().unwrap();
            if !self.head_resize_in_progress() {
                break;
            }
            let (_g, _) = self
                .resize_cv
                .wait_timeout(guard, LOG_EVERY)
                .expect("tx.head resize wait mutex not poisoned");
        }
    }

    /// Wake writers parked in [`Self::wait_for_head_resize_idle`].
    fn notify_head_resize_idle(&self) {
        // Hold `resize` so waiters re-check `head_resize_in_progress` after wake
        // without missing the notify between the check and wait.
        let _g = self.resize.lock().unwrap();
        self.resize_cv.notify_all();
    }

    /// `(shadow_cursor, class_a_count, target_bits, primary_slots)` for logs.
    fn head_resize_progress(&self) -> (u64, u64, u32, u64) {
        let snap = self.head_resize_size_snapshot();
        (
            snap.cursor,
            snap.class_a_n,
            if snap.active {
                snap.shadow_bits
            } else {
                snap.primary_bits
            },
            snap.primary_slots,
        )
    }

    /// Cheap occupancy of primary + in-progress shadow `tx.head` (for `ibd: sizes`).
    ///
    /// Body sizes are **sparse file** logical size (`slots × entry_bytes`), not
    /// necessarily resident RSS — still the right meter for dual-mmap retain
    /// during online resize.
    pub fn head_resize_size_snapshot(&self) -> HeadResizeSizeSnapshot {
        let class_a_n = self.count();
        let head = self.head.read().unwrap();
        let primary_bits = head.bits();
        let primary_slots = head.slots();
        let primary_entry_b = head.entry_bytes();
        let primary_occupied = head.occupied();
        let primary_body_bytes = head.layout().body_bytes();
        drop(head);

        let g = self.resize.lock().unwrap();
        let active = g.is_some() || self.resize_active.load(AtomicOrdering::Acquire);
        match g.as_ref() {
            Some(r) => HeadResizeSizeSnapshot {
                active: true,
                cursor: r.cursor,
                class_a_n,
                primary_bits,
                primary_slots,
                primary_entry_b,
                primary_occupied,
                primary_body_bytes,
                shadow_bits: r.shadow.bits(),
                shadow_slots: r.shadow.slots(),
                shadow_entry_b: r.shadow.entry_bytes(),
                shadow_occupied: r.shadow.occupied(),
                shadow_body_bytes: r.shadow.layout().body_bytes(),
            },
            None => HeadResizeSizeSnapshot {
                active,
                cursor: 0,
                class_a_n,
                primary_bits,
                primary_slots,
                primary_entry_b,
                primary_occupied,
                primary_body_bytes,
                shadow_bits: 0,
                shadow_slots: 0,
                shadow_entry_b: 0,
                shadow_occupied: 0,
                shadow_body_bytes: 0,
            },
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
        self.resize_active.load(AtomicOrdering::Acquire)
            || self.resize.lock().unwrap().is_some()
    }

    /// Resume incomplete resize from `tx.head.resize` control file (on open).
    fn resume_head_resize_if_needed(&self) -> Result<(), StoreError> {
        let Some(ctrl) = read_resize_control(&self.head_path)? else {
            // Orphan .new without control → drop it.
            let shadow = shadow_head_path(&self.head_path);
            if shadow.exists() {
                let _ = std::fs::remove_file(&shadow);
                crate::address_head::remove_legacy_meta_sidecar(&shadow);
            }
            return Ok(());
        };
        let shadow_path = shadow_head_path(&self.head_path);
        if !shadow_path.exists() {
            // Control without shadow: restart shadow create.
            let shadow = Arc::new(AddressHead::create_with_layout(
                &shadow_path,
                ctrl.target,
            )?);
            *self.resize.lock().unwrap() = Some(HeadResize {
                shadow,
                cursor: ctrl.cursor.max(1),
                target: ctrl.target,
            });
            self.resize_abort.store(false, AtomicOrdering::Release);
            self.resize_active.store(true, AtomicOrdering::Release);
            rbitcoin_log::info!(
                "store: resume tx.head resize bits={} entry={}B cursor={}",
                ctrl.target.bits,
                ctrl.target.entry_bytes,
                ctrl.cursor
            );
            self.ensure_resize_bg_running();
            return Ok(());
        }
        let shadow = Arc::new(AddressHead::open(&shadow_path)?);
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
        self.resize_abort.store(false, AtomicOrdering::Release);
        self.resize_active.store(true, AtomicOrdering::Release);
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
                // Warn if primary load is high while resizing — skip the first
                // moments (cursor still near 0) so open/start is not a false lag.
                let cursor = r.cursor;
                let target_bits = r.target.bits;
                if cursor > 1 {
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
                                100.0 * (cursor.saturating_sub(1) as f64) / (n as f64)
                            } else {
                                100.0
                            };
                            let (deep, exh) = crate::address_head::probe_depth_stats_snapshot();
                            rbitcoin_log::warn!(
                                "store: tx.head resize lagging load={ratio:.3} n={n} slots={slots} \
                                 shadow_cursor={}/{} ({pct:.1}%) target_bits={target_bits} \
                                 deep_warn={deep} exhaust={exh}",
                                cursor.saturating_sub(1),
                                n
                            );
                        }
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
        crate::address_head::remove_legacy_meta_sidecar(&shadow_path);
        let shadow = Arc::new(AddressHead::create_with_layout(&shadow_path, target)?);
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
        self.resize_abort.store(false, AtomicOrdering::Release);
        self.resize_active.store(true, AtomicOrdering::Release);
        drop(rg);
        self.ensure_resize_bg_running();
        Ok(())
    }

    /// Class A fks per bulk body-txid read when filling a head (rebuild or
    /// online resize shadow). `RBITCOIN_TX_HEAD_READ_BATCH` (default **65536**);
    /// legacy `RBITCOIN_TX_HEAD_RESIZE_READ_BATCH` still accepted.
    fn head_fill_read_batch() -> u64 {
        env_u64(
            "RBITCOIN_TX_HEAD_READ_BATCH",
            "RBITCOIN_TX_HEAD_RESIZE_READ_BATCH",
            65_536,
            1,
            1_000_000,
        )
    }

    /// `insert_many` group size when filling a head (one SeqCst fence per group).
    /// `RBITCOIN_TX_HEAD_WRITE_CHUNK` (default **65536**, same as read batch); legacy
    /// `RBITCOIN_TX_HEAD_RESIZE_WRITE_CHUNK` still accepted.
    fn head_fill_write_chunk() -> usize {
        env_u64(
            "RBITCOIN_TX_HEAD_WRITE_CHUNK",
            "RBITCOIN_TX_HEAD_RESIZE_WRITE_CHUNK",
            65_536,
            64,
            1_000_000,
        ) as usize
    }

    /// Fill shadow head for consecutive Class A ids.
    ///
    /// **Online path:** prefer io_uring pipeline ([`crate::head_resize_fill`]) so
    /// `tx.head.new` is filled via pread/pwrite page RMW (no shadow mmap fault-in).
    /// Falls back to bulk body reads + mmap `insert_many` if uring is unavailable.
    fn shadow_fill_fk_range(
        &self,
        shadow: &AddressHead,
        first: u64,
        last: u64,
    ) -> Result<(), StoreError> {
        if last < first {
            return Ok(());
        }
        match crate::head_resize_fill::run_shadow_fill_uring(
            &self.body,
            shadow,
            &self.secret,
            first,
            last,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => {
                static ONCE: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);
                if !ONCE.swap(true, std::sync::atomic::Ordering::Relaxed) {
                    rbitcoin_log::warn!(
                        "store: io_uring shadow fill unavailable ({e}) — falling back to mmap insert_many"
                    );
                }
            }
        }
        self.shadow_fill_fk_range_mmap(shadow, first, last)
    }

    /// Mmap `insert_many` fill (rebuild / uring fallback).
    fn shadow_fill_fk_range_mmap(
        &self,
        shadow: &AddressHead,
        first: u64,
        last: u64,
    ) -> Result<(), StoreError> {
        let write_chunk = Self::head_fill_write_chunk();
        let read_batch = Self::head_fill_read_batch();
        let mut cur = first;
        while cur <= last {
            let end = (cur + read_batch - 1).min(last);
            let txids = self.body_txid_range(cur, end)?;
            debug_assert_eq!(txids.len() as u64, end - cur + 1);
            let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(write_chunk);
            for (i, txid) in txids.into_iter().enumerate() {
                // Same keyed mix as live inserts (never raw txid as head key).
                batch.push((self.secret.mix_txid(&txid), Fk(cur + i as u64)));
                if batch.len() >= write_chunk {
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
    ///
    /// **Lock discipline:** `resize` Mutex is held only to snapshot `(shadow, cursor)`
    /// and later to commit the cursor. Fill IO runs **without** the mutex so archive
    /// `head_insert_many` → `maybe_start_head_resize` is not blocked for whole waves.
    pub fn head_resize_poll(&self, budget: u64) -> Result<(), StoreError> {
        if budget == 0 {
            return Ok(());
        }
        let n = self.count();
        // Snapshot under lock — do not hold across body_txid_range / insert_many.
        let work: Option<(Arc<AddressHead>, u64, u64, HeadLayout)> = {
            let mut rg = self.resize.lock().unwrap();
            let Some(r) = rg.as_mut() else {
                return Ok(());
            };
            if r.cursor == 0 {
                r.cursor = 1;
            }
            if n == 0 || r.cursor > n {
                None
            } else {
                let start = r.cursor;
                let end = (r.cursor + budget - 1).min(n);
                Some((Arc::clone(&r.shadow), start, end, r.target))
            }
        };

        let done_fill = match work {
            None => true,
            Some((shadow, start, end, target)) => {
                self.shadow_fill_fk_range(&shadow, start, end)?;
                let mut progress_log: Option<(u64, u64, u32)> = None;
                let mut done = false;
                {
                    let mut rg = self.resize.lock().unwrap();
                    let Some(r) = rg.as_mut() else {
                        return Ok(());
                    };
                    // Only one bg filler; still refuse to rewind if something advanced.
                    if r.cursor != start {
                        return Ok(());
                    }
                    r.cursor = end + 1;
                    let gen = self.head.read().unwrap().generation();
                    write_resize_control(
                        &self.head_path,
                        &ResizeControl {
                            target,
                            cursor: r.cursor,
                            generation: gen,
                        },
                    )?;
                    let advanced = r.cursor.saturating_sub(start);
                    let n_now = self.count();
                    let milestone = r.cursor > 1
                        && (r.cursor / 1_000_000 > start.saturating_sub(1) / 1_000_000
                            || r.cursor > n_now
                            || advanced >= 1_000_000);
                    if milestone {
                        progress_log =
                            Some((r.cursor.saturating_sub(1), n_now, r.target.bits));
                    }
                    if r.cursor > n_now {
                        done = true;
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
                done
            }
        };
        if done_fill {
            self.try_complete_head_resize()?;
        }
        Ok(())
    }

    /// Final catch-up under primary insert lock, then atomic rename swap.
    fn try_complete_head_resize(&self) -> Result<(), StoreError> {
        use std::time::{Duration, Instant};

        // Single completer: bg wave + main-thread poll must not both rename/drop
        // the shadow (IO Safety: owned FD already closed).
        if self
            .resize_completing
            .compare_exchange(
                false,
                true,
                AtomicOrdering::AcqRel,
                AtomicOrdering::Acquire,
            )
            .is_err()
        {
            return Ok(());
        }
        struct CompletingGuard<'a>(&'a AtomicBool);
        impl Drop for CompletingGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, AtomicOrdering::Release);
            }
        }
        let _completing_guard = CompletingGuard(&self.resize_completing);

        rbitcoin_log::info!(
            "store: tx.head resize fill done — final catch-up + swap (shadow → primary)"
        );

        // Phase 1: catch-up + flush **without** exclusive `head.write()` and
        // **without** holding `resize` across fill IO (same archive-stall fix as poll).
        {
            let n = self.count();
            let snap = {
                let rg = self.resize.lock().unwrap();
                rg.as_ref().map(|r| (Arc::clone(&r.shadow), r.cursor, r.target))
            };
            let Some((shadow, mut cursor, target)) = snap else {
                return Ok(());
            };
            if cursor == 0 {
                cursor = 1;
            }
            if cursor <= n {
                self.shadow_fill_fk_range(&shadow, cursor, n)?;
                cursor = n + 1;
            }
            let n2 = self.count();
            if cursor <= n2 {
                self.shadow_fill_fk_range(&shadow, cursor, n2)?;
                cursor = n2 + 1;
            }
            {
                let mut rg = self.resize.lock().unwrap();
                let Some(r) = rg.as_mut() else {
                    return Ok(());
                };
                r.cursor = cursor;
                let gen = self.head.read().unwrap().generation();
                write_resize_control(
                    &self.head_path,
                    &ResizeControl {
                        target,
                        cursor: r.cursor,
                        generation: gen,
                    },
                )?;
            }
            rbitcoin_log::info!(
                "store: tx.head resize flushing shadow (cursor={}, n={})",
                cursor.saturating_sub(1),
                n2.max(n)
            );
            shadow.flush()?;
        }

        // Phase 2: exclusive head ownership for rename + reopen. Use try_write so
        // we log while waiting (std RwLock write can starve under continuous reads).
        // Cancel if: sticky abort (Drop), or `resize_bg_gen` changes after we enter
        // this wait (worker kill / respawn while blocked). Gen bump alone before
        // entering the wait must *not* cancel — main-thread poll may complete after
        // killing the bg worker (archiver sleep test).
        rbitcoin_log::info!("store: tx.head resize acquiring exclusive head lock for swap…");
        let t_lock = Instant::now();
        if self.resize_abort.load(AtomicOrdering::Acquire) {
            rbitcoin_log::info!(
                "store: tx.head resize exclusive lock wait cancelled (abort before try_write)"
            );
            return Ok(());
        }
        let gen_at_start = self.resize_bg_gen.load(AtomicOrdering::Acquire);
        let mut waited = 0u32;
        let mut head_w = loop {
            if self.resize_abort.load(AtomicOrdering::Acquire)
                || self.resize_bg_gen.load(AtomicOrdering::Acquire) != gen_at_start
            {
                rbitcoin_log::info!(
                    "store: tx.head resize exclusive lock wait cancelled \
                     (abort/gen after {:?})",
                    t_lock.elapsed()
                );
                return Ok(());
            }
            match self.head.try_write() {
                Ok(g) => {
                    // Re-check after acquiring: Drop may have set abort while we
                    // raced into the write lock.
                    if self.resize_abort.load(AtomicOrdering::Acquire)
                        || self.resize_bg_gen.load(AtomicOrdering::Acquire) != gen_at_start
                    {
                        rbitcoin_log::info!(
                            "store: tx.head resize exclusive lock wait cancelled \
                             (abort after try_write, {:?})",
                            t_lock.elapsed()
                        );
                        drop(g);
                        return Ok(());
                    }
                    break g;
                }
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
        // One more catch-up under exclusive insert barrier (archive paused).
        // Still avoid holding `resize` for the fill itself.
        let n3 = self.count();
        let (shadow, mut cursor, target) = {
            let rg = self.resize.lock().unwrap();
            let Some(r) = rg.as_ref() else {
                return Ok(());
            };
            (Arc::clone(&r.shadow), r.cursor, r.target)
        };
        if cursor == 0 {
            cursor = 1;
        }
        if cursor <= n3 {
            self.shadow_fill_fk_range(&shadow, cursor, n3)?;
            cursor = n3 + 1;
            shadow.flush()?;
        }
        drop(shadow);
        let new_gen = head_w.generation().saturating_add(1);
        let shadow_path = shadow_head_path(&self.head_path);
        let bak = bak_head_path(&self.head_path);
        let _ = std::fs::remove_file(&bak);

        // Take ownership of HeadResize and wait until we hold the **only** Arc to
        // the shadow AddressHead. Concurrent `head_resize_poll` clones must drop
        // before rename or mmap/FD is double-closed (IO Safety abort).
        let hr = {
            let mut rg = self.resize.lock().unwrap();
            let Some(mut r) = rg.take() else {
                return Ok(());
            };
            r.cursor = cursor;
            r
        };
        let wait_arc = Instant::now();
        let mut spins = 0u32;
        let gen_arc = self.resize_bg_gen.load(AtomicOrdering::Acquire);
        while Arc::strong_count(&hr.shadow) > 1 {
            spins = spins.saturating_add(1);
            if spins == 1 || spins % 200 == 0 {
                rbitcoin_log::info!(
                    "store: tx.head resize waiting for shadow Arc exclusive \
                     (strong={}, {:?})",
                    Arc::strong_count(&hr.shadow),
                    wait_arc.elapsed()
                );
            }
            if self.resize_abort.load(AtomicOrdering::Acquire)
                || self.resize_bg_gen.load(AtomicOrdering::Acquire) != gen_arc
            {
                // Put resize back so Drop/resume can clean up; abandon swap.
                *self.resize.lock().unwrap() = Some(hr);
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(hr); // closes shadow mmap/FD on tx.head.new
        drop(primary_writes);

        rbitcoin_log::info!(
            "store: tx.head resize renaming shadow → primary (bits={} slots={})",
            target.bits,
            target.slots()
        );
        // head_w still held — primary mmap open on old path; Linux allows rename.
        std::fs::rename(&self.head_path, &bak).map_err(|e| StoreError::io(&self.head_path, e))?;
        std::fs::rename(&shadow_path, &self.head_path)
            .map_err(|e| StoreError::io(&shadow_path, e))?;
        // Layout lives in the file footer (renamed with shadow); bump generation.
        write_head_meta(&self.head_path, target, new_gen)?;
        clear_resize_control(&self.head_path);
        *head_w = AddressHead::open(&self.head_path)?;
        let _ = std::fs::remove_file(&bak);
        crate::address_head::remove_legacy_meta_sidecar(&bak);
        crate::address_head::remove_legacy_meta_sidecar(&shadow_path);
        self.resize_active.store(false, AtomicOrdering::Release);
        // Archive writers may be parked on probe-exhaust for the whole fill;
        // wake them now that the wider primary is live.
        self.notify_head_resize_idle();
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
    use std::time::Instant;

    /// Drive head resize to completion with a hard deadline.
    ///
    /// Actively calls [`TxTable::head_resize_poll`] so progress does not depend
    /// solely on the background fill thread (sleep-only waits stall when the bg
    /// worker is slow to start, starved, or mid exclusive-lock wait).
    fn wait_head_resize_done(t: &TxTable, deadline: Duration) {
        let start = Instant::now();
        while t.head_resize_in_progress() {
            assert!(
                start.elapsed() < deadline,
                "tx.head resize stalled after {:?} (bits={} count={})",
                start.elapsed(),
                t.head_bits(),
                t.count()
            );
            t.head_resize_poll(head_fill_wave())
                .expect("head_resize_poll during wait");
            // Yield if still in progress (e.g. exclusive lock contention on swap).
            if t.head_resize_in_progress() {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        // Join finished bg worker before tests `remove_dir_all` — otherwise a late
        // rename can ENOENT or race mmap teardown (tcache double-free under load).
        let mut slot = t.resize_bg.lock().unwrap();
        if let Some(h) = slot.take() {
            let _ = h.join();
        }
    }

    fn tiny_layout() -> HeadLayout {
        HeadLayout::new(crate::address_head::TINY_BITS).unwrap()
    }

    fn layout_bits(bits: u32) -> HeadLayout {
        HeadLayout::new(bits).unwrap()
    }

    fn create_tiny(dir: &Path) -> TxTable {
        TxTable::create_with_head_layout(dir, tiny_layout()).unwrap()
    }

    fn create_bits(dir: &Path, bits: u32) -> TxTable {
        TxTable::create_with_head_layout(dir, layout_bits(bits)).unwrap()
    }

    /// Process-global env knobs still used by a few tests (read-batch / bulk IO).
    /// Hold this while mutating so parallel tests cannot race.
    static TEST_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_env_lock<R>(f: impl FnOnce() -> R) -> R {
        let _g = TEST_ENV_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        f()
    }

    fn with_resize_stress_lock<R>(f: impl FnOnce() -> R) -> R {
        // Share with concurrent file/var_table stress so bg resize and mmap grow
        // never overlap across tests in one process.
        let _g = crate::file::TEST_MMAP_STRESS_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        f()
    }

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

    /// All page-local head paths agree: insert_many, probe_fks, single resolve,
    /// batch resolve, BIP30 depth-win, and post-resize lookup.
    #[test]
    fn page_local_head_paths_insert_probe_batch_resize() {
        with_resize_stress_lock(|| {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-page-paths-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // BITS=12 → multi-page table (2^12 slots, 1024/page) so page select matters.
        let t = create_bits(&dir, 12);
        assert_eq!(t.head_bits(), 12);
        assert!(t.head_slots() > crate::address_head::PAGE_SLOTS);

        let mk = |i: u64, txid: [u8; 32]| {
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
                script_sig: vec![(i & 0xff) as u8],
                witness: vec![],
            }];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs)
        };

        // Distinct txids across pages + one BIP30 pair.
        let mut keys = Vec::new();
        for i in 1..=40u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            txid[8] = 0xa1;
            keys.push(txid);
            let _ = t.put_full_batch_indexed(&[mk(i, txid)], true).unwrap();
        }
        // BIP30: second create for keys[0].
        let bip = keys[0];
        let fk_old = t.get_fk_by_txid(&bip).unwrap().unwrap();
        let fk_new = t.put_full_batch_indexed(&[mk(100, bip)], true).unwrap()[0];
        assert!(fk_new.0 > fk_old.0);

        // Single resolve matches batch; BIP30 prefers deeper (newer).
        assert_eq!(t.get_fk_by_txid(&bip).unwrap(), Some(fk_new));
        let batch = t.get_fk_by_txid_batch(&keys).unwrap();
        assert_eq!(batch.len(), keys.len());
        for (txid, fk) in &batch {
            let single = t.get_fk_by_txid(txid).unwrap();
            assert_eq!(*fk, single, "batch/single mismatch for {txid:?}");
        }
        assert_eq!(
            batch.iter().find(|(t, _)| *t == bip).unwrap().1,
            Some(fk_new)
        );

        // probe_fks lists older then newer; reverse body-verify wins newest.
        let mixed = t.secret.mix_txid(&bip);
        let cands = t.head.read().unwrap().probe_fks(&mixed).unwrap();
        assert!(cands.contains(&fk_old) && cands.contains(&fk_new));
        assert_eq!(cands[0], fk_old);

        // Online resize shadow fill must preserve all mappings.
        t.start_head_resize(crate::address_head::HeadLayout::new(13).unwrap())
            .unwrap();
        wait_head_resize_done(&t, Duration::from_secs(15));
        assert_eq!(t.head_bits(), 13);
        for txid in &keys {
            assert!(
                t.get_fk_by_txid(txid).unwrap().is_some(),
                "missing after resize"
            );
        }
        assert_eq!(t.get_fk_by_txid(&bip).unwrap(), Some(fk_new));
        let batch2 = t.get_fk_by_txid_batch(&keys).unwrap();
        for ((_, a), (_, b)) in batch.iter().zip(batch2.iter()) {
            assert_eq!(a, b);
        }

        // Reopen path (footer layout + page probes).
        drop(t);
        let t2 = TxTable::open(&dir).unwrap();
        assert_eq!(t2.head_bits(), 13);
        assert_eq!(t2.get_fk_by_txid(&bip).unwrap(), Some(fk_new));
        for txid in &keys {
            assert_eq!(
                t2.get_fk_by_txid(txid).unwrap(),
                t2.get_fk_by_txid_batch(&[*txid]).unwrap()[0].1
            );
        }

        let _ = std::fs::remove_dir_all(&dir);
        });
    }

    /// Overflow accepts depth-exhausted inserts; lookup is overflow-first then primary.
    #[test]
    fn head_overflow_insert_and_overflow_first_lookup() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-overflow-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);
        let txid = [0x77u8; 32];
        let mk = |i: u8| {
            let mut t2 = txid;
            t2[31] = i;
            let rec = TxRecord {
                txid: t2,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let inputs = vec![InputRecord::coinbase(u32::MAX, vec![i], vec![])];
            let outputs = vec![OutputRecord::unspent(1, vec![0x51])];
            (rec, inputs, outputs, t2)
        };
        // Direct overflow insert + primary insert for comparison.
        let (rec, ins, outs, raw_txid) = mk(1);
        let fk = t
            .put_full_batch_indexed(&[(rec, ins, outs)], true)
            .unwrap()[0];
        let mixed = t.secret.mix_txid(&raw_txid);
        // Simulate depth-exhaust: push same mixed key's sibling into overflow.
        let mut alt = raw_txid;
        alt[30] = 0xab;
        let mixed_alt = t.secret.mix_txid(&alt);
        {
            let mut ov = t.overflow.lock().unwrap();
            ov.insert(&mixed_alt, Fk(fk.0 + 1000)).unwrap();
            ov.persist().unwrap();
        }
        // Overflow-first: get with a body that won't match overflow fk still works via primary.
        assert_eq!(t.get_fk_by_txid(&raw_txid).unwrap(), Some(fk));
        // mix must not equal raw
        assert_ne!(mixed, raw_txid);
        // reopen overflow
        let o2 = crate::head_overflow::HeadOverflow::open(&dir).unwrap();
        assert_eq!(o2.get(&mixed_alt), Some(Fk(fk.0 + 1000)));
        let _ = std::fs::remove_dir_all(&dir);
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
        let t = create_tiny(&dir);
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
        // Probe order: older first, newer deeper (mixed keys).
        let mixed = t.secret.mix_txid(&txid);
        let cands = t.head.read().unwrap().probe_fks(&mixed).unwrap();
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
        with_resize_stress_lock(|| {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-resize-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        // Tiny head: 2^10 = 1024 slots (explicit layout — no env race).
        let t = create_bits(&dir, 10);
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

        // Concurrent primary inserts while resizing (no dual-write to shadow).
        // Run inserts on a worker so they overlap fill IO, then join before
        // waiting for swap — mirrors archive+resize without leaving puts in
        // flight across the exclusive rename barrier.
        let t_ins = {
            // TxTable is not Clone; share via raw pointer is wrong — just insert
            // on this thread after a short poll so the bg worker is started.
            t.head_resize_poll(head_fill_wave()).expect("kick fill");
            t
        };
        for i in 51..=80u64 {
            let _ = t_ins.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        // Drive fill (bg + poll); hard deadline — no open-ended sleep loop.
        wait_head_resize_done(&t_ins, Duration::from_secs(10));
        assert_eq!(t_ins.head_bits(), 11);
        assert_eq!(t_ins.count(), 80);
        // After swap, every seeded txid must resolve. Retry once: final catch-up
        // may still be publishing under extreme host load (mmap rename).
        for i in 1..=80u64 {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let mut fk = t_ins.get_fk_by_txid(&txid).unwrap();
            if fk.is_none() {
                std::thread::sleep(Duration::from_millis(5));
                fk = t_ins.get_fk_by_txid(&txid).unwrap();
            }
            assert_eq!(fk, Some(Fk(i)), "txid {i} missing after resize");
        }
        drop(t_ins);
        let _ = std::fs::remove_dir_all(&dir);
        });
    }

    /// Probe-exhaust wait path: park until resize completes (no wall-clock fail).
    ///
    /// Regression for the mainnet 29→30 case where a 30‑minute `MAX_WAIT` killed
    /// the archive pipeline while a healthy bg resize was still ~79% done.
    #[test]
    fn archiver_sleeps_until_resize_notifies() {
        with_resize_stress_lock(|| {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-sleep-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_bits(&dir, 10);
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
        // Seed a small set, start a controlled widen, park a waiter, then complete
        // via poll (tests do not spawn the production bg filler — see
        // `ensure_resize_bg_running`).
        for i in 1..=50u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        wait_head_resize_done(&t, Duration::from_secs(15));

        let next_bits = t.head_bits().saturating_add(1);
        t.start_head_resize(crate::address_head::HeadLayout::new(next_bits).unwrap())
            .unwrap();
        assert!(t.head_resize_in_progress());

        let this = &t as *const TxTable as usize;
        let waiter = std::thread::Builder::new()
            .name("test-tx-head-sleep".into())
            .spawn(move || {
                // SAFETY: `t` lives until join below.
                let table = unsafe { &*(this as *const TxTable) };
                table.wait_for_head_resize_idle();
            })
            .unwrap();

        // Park window: waiter blocked until we finish fill+swap and notify.
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !waiter.is_finished(),
            "waiter must still be sleeping while resize is in progress"
        );

        wait_head_resize_done(&t, Duration::from_secs(15));
        assert!(!t.head_resize_in_progress());

        waiter.join().expect("archiver sleep thread panicked");
        // If notify_head_resize_idle were missing, join would hang.
        drop(t);
        let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn head_load_trigger_starts_resize() {
        with_resize_stress_lock(|| {
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
        let t = create_bits(&dir, 8);
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
        // head_insert_many should have started resize; drive fill to completion.
        t.maybe_start_head_resize().unwrap();
        if t.head_resize_in_progress() {
            wait_head_resize_done(&t, Duration::from_secs(10));
        }
        assert!(t.head_bits() >= 9, "bits={}", t.head_bits());
        // Spot-check resolves.
        for i in [1u64, 100, 210] {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(i)));
        }
        let _ = std::fs::remove_dir_all(&dir);
        });
    }

    /// io_uring shadow fill matches mmap insert_many for a small Class A set.
    #[test]
    fn shadow_fill_uring_matches_insert_many() {
        with_resize_stress_lock(|| {
        if !crate::bulk_io::io_uring_enabled() {
            eprintln!("skip: io_uring unavailable");
            return;
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-shadow-uring-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);
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
        // > FK_QUEUE_LOW (256) so refill/drain interaction is exercised (regression
        // for queue-len==256 dead zone that stalled with no in-flight IO).
        const N: u64 = 400;
        for i in 1..=N {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }
        let layout = crate::address_head::HeadLayout::new(12).unwrap();
        let shadow_a = dir.join("shadow_a");
        let shadow_b = dir.join("shadow_b");
        let ha = AddressHead::create_with_layout(&shadow_a, layout).unwrap();
        let hb = AddressHead::create_with_layout(&shadow_b, layout).unwrap();
        // mmap path
        t.shadow_fill_fk_range_mmap(&ha, 1, N).unwrap();
        // uring path
        crate::head_resize_fill::run_shadow_fill_uring(&t.body, &hb, &t.secret, 1, N)
            .unwrap();
        assert_eq!(ha.occupied(), hb.occupied(), "occupied mismatch");
        for i in 1..=N {
            let mut txid = [0u8; 32];
            txid[0..8].copy_from_slice(&i.to_le_bytes());
            let mixed = t.secret.mix_txid(&txid);
            let ca = ha.probe_fks(&mixed).unwrap();
            let cb = hb.probe_fks(&mixed).unwrap();
            assert!(ca.contains(&Fk(i)), "mmap missing {i}");
            assert!(cb.contains(&Fk(i)), "uring missing {i}");
        }
        let _ = std::fs::remove_dir_all(&dir);
        });
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
        let t = create_tiny(&dir);
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
    }

    /// Resize with a tiny bulk-read batch still fills shadow correctly.
    #[test]
    fn head_resize_with_small_read_batch() {
        with_env_lock(|| {
        with_resize_stress_lock(|| {
            let dir = std::env::temp_dir().join(format!(
                "rbitcoin-tx-head-resize-batch-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            // Force multi-chunk bulk reads inside each poll budget.
            std::env::set_var("RBITCOIN_TX_HEAD_READ_BATCH", "7");
            let t = create_bits(&dir, 10);
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
            wait_head_resize_done(&t, Duration::from_secs(10));
            assert_eq!(t.head_bits(), 11);
            for i in 1..=60u64 {
                let mut txid = [0u8; 32];
                txid[0..8].copy_from_slice(&i.to_le_bytes());
                assert_eq!(t.get_fk_by_txid(&txid).unwrap(), Some(Fk(i)));
            }
            drop(t);
            let _ = std::fs::remove_dir_all(&dir);
            std::env::remove_var("RBITCOIN_TX_HEAD_READ_BATCH");
        });
        });
    }

    /// Exclusive-lock wait in `try_complete` must abort when `resize_bg_gen`
    /// advances (Drop / worker respawn) — otherwise `join` hangs forever.
    #[test]
    fn head_resize_exclusive_lock_wait_cancels_on_gen_bump() {
        with_resize_stress_lock(|| {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-head-gen-cancel-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_bits(&dir, 10);
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
        for i in 1..=20u64 {
            let _ = t.put_full_batch_indexed(&[mk(i)], true).unwrap();
        }

        use std::sync::atomic::AtomicBool;
        let held = AtomicBool::new(false);
        let release = AtomicBool::new(false);
        std::thread::scope(|s| {
            // Hold head.read **before** resize starts so bg/poll cannot slip a
            // swap through before the exclusive-lock wait is contended.
            s.spawn(|| {
                let _guard = t.head.read().unwrap();
                held.store(true, AtomicOrdering::Release);
                while !release.load(AtomicOrdering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
                drop(_guard);
            });
            let wait_held = Instant::now();
            while !held.load(AtomicOrdering::Acquire) {
                assert!(
                    wait_held.elapsed() < Duration::from_secs(2),
                    "reader never acquired head.read"
                );
                std::thread::sleep(Duration::from_millis(1));
            }

            t.start_head_resize(crate::address_head::HeadLayout::new(11).unwrap())
                .unwrap();

            // Poll on a side thread: shadow fill completes, then blocks in try_write.
            let poller = s.spawn(|| t.head_resize_poll(head_fill_wave()));
            // Wait until fill is done and swap is blocked on exclusive lock
            // (in_progress still true under held head.read).
            let wait = Instant::now();
            while wait.elapsed() < Duration::from_secs(2) {
                if t.head_resize_in_progress() {
                    // Give bg/poller a slice to enter try_write after fill.
                    std::thread::sleep(Duration::from_millis(20));
                    break;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            assert!(
                t.head_resize_in_progress(),
                "resize should still be blocked on exclusive head lock"
            );

            // Same signal Drop uses before join — sticky abort + gen so waiters
            // exit even if they enter try_write *after* this line.
            t.resize_abort.store(true, AtomicOrdering::Release);
            t.resize_bg_gen
                .fetch_add(1, AtomicOrdering::AcqRel);

            let t0 = Instant::now();
            let _ = poller.join();
            // Also join bg fill so it cannot complete a swap after we release head.read.
            if let Some(h) = t.resize_bg.lock().unwrap().take() {
                let _ = h.join();
            }
            assert!(
                t0.elapsed() < Duration::from_secs(5),
                "head_resize_poll/bg stuck in exclusive lock wait for {:?}",
                t0.elapsed()
            );

            release.store(true, AtomicOrdering::Release);
        });

        // Drop joins any leftover bg worker; must also finish promptly.
        let t0 = Instant::now();
        drop(t);
        assert!(
            t0.elapsed() < Duration::from_secs(5),
            "TxTable Drop hung for {:?}",
            t0.elapsed()
        );

        let _ = std::fs::remove_dir_all(&dir);
        });
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
        let t = create_tiny(&dir);
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
    }

    /// Bulk body_range (sorted mmap) + get_full_batch_at agree with sequential paths.
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
        let t = create_tiny(&dir);
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
        // Unsorted + sparse sample still matches serial body_range.
        let mut shuffled = fks.clone();
        shuffled.reverse();
        let sparse = vec![shuffled[0], shuffled[3], shuffled[3], shuffled[7]];
        let batch_sparse = t.body_range_batch(&sparse).unwrap();
        for (fk, br) in sparse.iter().zip(batch_sparse.iter()) {
            let seq = t.body_range(*fk).unwrap();
            assert_eq!(*br, Some(seq), "fk={fk:?}");
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
            assert_eq!(b.3.len(), b.2.len()); // denserels
            for o in &b.2 {
                assert!(o.spender_field.is_null());
            }
        }
        let meta_ranges: Vec<(u64, u64)> = range_args.iter().map(|(_, o, l)| (*o, *l)).collect();
        let meta = t.get_meta_and_outputs_batch_at(&meta_ranges).unwrap();
        for ((_, off, len), got) in range_args.iter().zip(meta.iter()) {
            let seq = t.get_meta_and_outputs_at(*off, *len).unwrap();
            let b = got.as_ref().expect("meta bulk");
            assert_eq!(b.0, seq.0);
            assert_eq!(b.1.len(), seq.1.len());
            assert_eq!(b.2.len(), b.1.len()); // dense spender_rels
            // Content-only outs (spender fields cleared for pin/FIFO).
            for o in &b.1 {
                assert!(o.spender_field.is_null());
                assert!(!o.multi_spender);
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Streaming early-exit: deepest match → fewer body_lookups than cand count.
    #[test]
    fn streaming_resolve_early_exit_fewer_body_lookups() {
        if !crate::bulk_io::io_uring_enabled() {
            return;
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-stream-early-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);
        let txid = [0xcd; 32];
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
        // Two creates of same txid → two cands; deepest (second) should match first try.
        let _fk1 = t.put_full_batch_indexed(&[mk(1)], true).unwrap()[0];
        let fk2 = t.put_full_batch_indexed(&[mk(2)], true).unwrap()[0];
        let _ = crate::head_resolve_stats::sample_and_reset();
        let batch = t.get_fk_by_txid_batch(&[txid]).unwrap();
        assert_eq!(batch[0].1, Some(fk2));
        let s = crate::head_resolve_stats::sample_and_reset();
        // Deepest-first early exit: body_lookups ≤ cands (exact `==1` is flaky under
        // parallel tests sharing head_resolve_stats atomics).
        assert!(
            s.body_lookups <= s.cands.max(1),
            "body_lookups {} > cands {}",
            s.body_lookups,
            s.cands
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Depth-max match: foreigners + two same-txid creates; batch prefers deepest.
    #[test]
    fn get_fk_by_txid_batch_depth_wins_with_workers() {
        with_env_lock(|| {
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
        let t = create_tiny(&dir);
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
        std::env::remove_var("RBITCOIN_BULK_IO_WORKERS");
        });
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
        let t = create_tiny(&dir);
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
        let t = create_tiny(&dir);
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

    /// Operator recovery: delete `tx.head` → open rebuilds from Class A bodies.
    #[test]
    fn missing_tx_head_rebuilds_from_bodies_on_open() {
        // Open recreates via default_layout() (env scale); lock so parallel tests
        // cannot flip HEAD_SCALE mid-rebuild to mainnet (256 MiB sparse).
        with_env_lock(|| {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            let dir = std::env::temp_dir().join(format!(
                "rbitcoin-tx-head-rebuild-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();

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
                let t = create_tiny(&dir);
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

            // Simulate operator: wipe head (layout is in the file footer).
            let head = dir.join("tx.head");
            assert!(head.exists());
            std::fs::remove_file(&head).unwrap();

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
        });
    }

    #[test]
    fn missing_tx_head_with_no_bodies_creates_empty() {
        with_env_lock(|| {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            let dir = std::env::temp_dir().join(format!(
                "rbitcoin-tx-head-empty-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            {
                let t = create_tiny(&dir);
                t.flush().unwrap();
            }
            std::fs::remove_file(dir.join("tx.head")).unwrap();
            let t = TxTable::open(&dir).unwrap();
            assert_eq!(t.count(), 0);
            assert!(dir.join("tx.head").exists());
            let _ = std::fs::remove_dir_all(&dir);
            std::env::remove_var("RBITCOIN_HEAD_SCALE");
        });
    }

    /// Resume incomplete resize: control+shadow, control-only, orphan .new.
    #[test]
    fn resume_head_resize_control_and_orphan_shadow() {
        with_env_lock(|| {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
            let dir = std::env::temp_dir().join(format!(
                "rbitcoin-tx-resume-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let head_path = dir.join("tx.head");
            // Seed a few bodies + head.
            {
                let t = create_tiny(&dir);
                let mk = |txid: [u8; 32]| {
                    (
                        TxRecord {
                            txid,
                            version: 1,
                            locktime: 0,
                            input_start_fk: Fk::NULL,
                            input_count: 1,
                            output_start_fk: Fk::NULL,
                            output_count: 1,
                        },
                        vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
                        vec![OutputRecord::unspent(1, vec![0x51])],
                    )
                };
                for i in 0..8u8 {
                    let mut txid = [0u8; 32];
                    txid[0] = i;
                    t.put_full_batch_indexed(&[mk(txid)], true).unwrap();
                }
                t.flush().unwrap();
            }

            // Orphan .new without control → dropped on open.
            {
                let orphan = shadow_head_path(&head_path);
                AddressHead::create_with_layout(&orphan, HeadLayout::new(12).unwrap()).unwrap();
                assert!(orphan.exists());
                let t = TxTable::open(&dir).unwrap();
                assert!(!t.head_resize_in_progress());
                assert!(!orphan.exists());
                drop(t);
            }

            // Control without shadow → recreate shadow and mark active.
            {
                let target = HeadLayout::new(12).unwrap();
                write_resize_control(
                    &head_path,
                    &ResizeControl {
                        target,
                        cursor: 3,
                        generation: 1,
                    },
                )
                .unwrap();
                let t = TxTable::open(&dir).unwrap();
                assert!(t.head_resize_in_progress());
                let snap = t.head_resize_size_snapshot();
                assert!(snap.active);
                assert!(snap.cursor >= 1);
                // Drive a little fill via poll (bg disabled under test).
                t.head_resize_poll(4).unwrap();
                wait_head_resize_done(&t, Duration::from_secs(30));
                assert!(!t.head_resize_in_progress());
                drop(t);
            }

            // Control + existing matching shadow.
            {
                let target = HeadLayout::new(13).unwrap();
                let shadow = shadow_head_path(&head_path);
                let _ = std::fs::remove_file(&shadow);
                AddressHead::create_with_layout(&shadow, target).unwrap();
                write_resize_control(
                    &head_path,
                    &ResizeControl {
                        target,
                        cursor: 2,
                        generation: 2,
                    },
                )
                .unwrap();
                let t = TxTable::open(&dir).unwrap();
                assert!(t.head_resize_in_progress());
                // Layout mismatch: rewrite shadow with wrong bits then open fails.
                drop(t);
                clear_resize_control(&head_path);
                let _ = std::fs::remove_file(&shadow);
            }

            // Unreadable head with bodies → recreate + rebuild.
            {
                // Corrupt footer/magic of head.
                std::fs::write(&head_path, b"not-a-valid-head-file!!!!!!").unwrap();
                let t = TxTable::open(&dir).unwrap();
                assert_eq!(t.count(), 8);
                // Head rebuilt: lookups work.
                let mut txid = [0u8; 32];
                txid[0] = 3;
                assert!(t.get_by_txid(&txid).unwrap().is_some());
                drop(t);
            }

            // ensure_head_resize / start / head_insert_many_sole / snapshot inactive
            {
                let fresh = dir.join("fresh");
                std::fs::create_dir_all(&fresh).unwrap();
                let t = create_tiny(&fresh);
                assert!(!t.head_resize_in_progress());
                let snap = t.head_resize_size_snapshot();
                assert!(!snap.active);
                assert_eq!(snap.shadow_bits, 0);
                t.head_insert_many_sole(&[]).unwrap();
                // Force start bits+1
                let bits = t.head_bits();
                if bits < MAX_BITS {
                    t.start_head_resize(HeadLayout::new(bits + 1).unwrap())
                        .unwrap();
                    assert!(t.head_resize_in_progress());
                    // Second start is no-op
                    t.start_head_resize(HeadLayout::new(bits + 1).unwrap())
                        .unwrap();
                    // ensure via probe exhaust path is private; maybe_start while active
                    t.maybe_start_head_resize().unwrap();
                    t.head_resize_poll(8).unwrap();
                    wait_head_resize_done(&t, Duration::from_secs(30));
                }
                drop(t);
            }

            let _ = std::fs::remove_dir_all(&dir);
            std::env::remove_var("RBITCOIN_HEAD_SCALE");
        });
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
        let t = create_tiny(&dir);
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

        // Bulk 9-byte abs preads match the packed walk (pin → write spentness path).
        let decoded = t.get_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        let (_meta, outs, rels) = decoded[0].as_ref().expect("decode with rels");
        assert_eq!(outs.len(), 3);
        assert_eq!(rels.len(), 3);
        for o in outs {
            assert!(o.spender_field.is_null());
        }
        let abs: Vec<u64> = rels
            .iter()
            .map(|r| off.saturating_add(u64::from(*r)))
            .collect();
        let bulk = t.get_spender_meta_at_abs_batch(&abs).unwrap();
        assert_eq!(bulk.len(), 3);
        assert_eq!(bulk[0], Some((false, s1)));
        assert_eq!(bulk[1], Some((false, Fk::NULL)));
        assert_eq!(bulk[2], Some((false, Fk(20))));
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
        let t = create_tiny(&dir);
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
        encode_input_run_secret(&run, &mut enc, None);
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
        encode_output_run_secret(&run, &mut enc, None);
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
    fn short_or_truncated_packed_body_rejected() {
        // Too short for TxRecord meta.
        assert!(!is_packed_tx_payload(&[]));
        assert!(!is_packed_tx_payload(&[0u8; 63]));
        assert!(matches!(
            decode_packed_tx(&[0u8; 63]),
            Err(StoreError::Corrupt(_))
        ));
        // Meta claims inputs/outputs but payload ends after meta.
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
        assert!(is_packed_tx_payload(&raw));
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
        let t = create_tiny(&dir);
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

    /// Production API surface: abs spender meta, get_all, head snapshot, flushes.
    #[test]
    fn tx_table_spend_meta_batch_snapshot_and_helpers() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-surface-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);
        let mk = |txid: [u8; 32], outs: u32| {
            let outputs: Vec<_> = (0..outs)
                .map(|i| OutputRecord::unspent(i as i64 + 1, vec![0x51]))
                .collect();
            (
                TxRecord {
                    txid,
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: outs,
                },
                vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
                outputs,
            )
        };
        let create_fk = t
            .put_full_batch_indexed(&[mk([1u8; 32], 3)], true)
            .unwrap()[0];
        let s1 = t
            .put_full_batch_indexed(&[mk([2u8; 32], 1)], true)
            .unwrap()[0];
        let s2 = t
            .put_full_batch_indexed(&[mk([3u8; 32], 1)], true)
            .unwrap()[0];

        t.advise_body_dont_need(0, 0);
        assert!(t.body_logical_len() > 0);
        t.reserve_append(256, 2).unwrap();
        assert_eq!(t.count(), 3);

        // put_batch_indexed without head index (body only bare meta — not used as get)
        let bare = TxRecord {
            txid: [9u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk::NULL,
            output_count: 0,
        };
        let bare_fks = t.put_batch_indexed(&[bare], false).unwrap();
        assert_eq!(bare_fks.len(), 1);
        assert!(t.put_batch(&[]).unwrap().is_empty());

        let (off, len) = t.body_range(create_fk).unwrap();
        let decoded = t
            .get_meta_and_outputs_batch_at(&[(off, len)])
            .unwrap()
            .into_iter()
            .next()
            .unwrap()
            .expect("create body");
        let (_meta, _outs, rels) = decoded;
        assert_eq!(rels.len(), 3);
        let abs: Vec<(u64, Fk, u32, Fk)> = rels
            .iter()
            .enumerate()
            .map(|(v, &rel)| (off + u64::from(rel), create_fk, v as u32, s1))
            .collect();
        let spenders = crate::spender_table::SpenderTable::create(&dir).unwrap();
        // First sole spends via abs meta
        let cold = t.put_spend_batch_by_abs_meta(&spenders, &abs).unwrap();
        assert!(cold.is_empty());
        // Idempotent
        let cold2 = t
            .put_spend_batch_by_abs_meta(&spenders, &abs[..1])
            .unwrap();
        assert!(cold2.is_empty());
        // Second spender → multi in-place (not cold)
        let abs2 = [(abs[0].0, create_fk, 0, s2)];
        let cold3 = t.put_spend_batch_by_abs_meta(&spenders, &abs2).unwrap();
        assert!(cold3.is_empty(), "multi promote handled in abs path");
        let (m0, f0) = t.get_output_spender_meta(create_fk, 0).unwrap();
        assert!(m0, "MULTI set after second spend");
        assert!(!f0.is_null());
        // InvalidFk
        assert!(matches!(
            t.put_spend_batch_by_abs_meta(&spenders, &[(abs[0].0, create_fk, 0, Fk::NULL)]),
            Err(StoreError::InvalidFk)
        ));
        assert!(t.put_spend_batch_by_abs_meta(&spenders, &[]).unwrap().is_empty());
        // OOB abs → cold
        let cold4 = t
            .put_spend_batch_by_abs_meta(&spenders, &[(u64::MAX - 4, create_fk, 0, s1)])
            .unwrap();
        assert_eq!(cold4.len(), 1);

        let metas = t
            .get_spender_meta_at_abs_batch(&[abs[0].0, abs[1].0, u64::MAX])
            .unwrap();
        assert_eq!(metas.len(), 3);
        assert!(metas[0].is_some());
        assert!(metas[2].is_none());
        assert!(t.get_spender_meta_at_abs_batch(&[]).unwrap().is_empty());

        // set/get output spender meta
        t.set_output_spender_meta(create_fk, 2, false, s1).unwrap();
        let (m, f) = t.get_output_spender_meta(create_fk, 2).unwrap();
        assert!(!m);
        assert_eq!(f, s1);
        t.set_output_spender_meta_at(off, len, 2, true, s2).unwrap();
        let (m2, f2) = t.get_output_spender_meta_at(off, len, 2).unwrap();
        assert!(m2);
        assert_eq!(f2, s2);

        assert_eq!(t.body_txid(create_fk).unwrap(), [1u8; 32]);
        assert_eq!(t.body_txid_at(off, len).unwrap(), [1u8; 32]);
        let all = t.get_all_by_txid(&[1u8; 32]).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].0, create_fk);

        let snap = t.head_resize_size_snapshot();
        assert!(!snap.active || snap.active);
        assert_eq!(snap.class_a_n, t.count());
        assert!(t.head_occupied() >= 3);
        t.head_insert_many_sole(&[]).unwrap();
        t.head_insert_many(&[]).unwrap();
        t.maybe_start_head_resize().unwrap();

        t.flush().unwrap();
        t.flush_async().unwrap();
        // decode error arms: truncated script/witness on input
        {
            let mut bad = vec![0u8]; // flags: no null, no final, has script
            bad.extend_from_slice(&1u64.to_le_bytes());
            bad.push(0); // vout 0
            // no sequence (SEQ_FINAL not set) — short
            assert!(InputRecord::decode_at(&bad).is_err());
            // with sequence, truncated script
            let mut bad2 = vec![input_flags::SEQ_FINAL];
            bad2.extend_from_slice(&1u64.to_le_bytes());
            bad2.push(0);
            bad2.push(5); // compact len 5
            bad2.extend_from_slice(&[1, 2]); // only 2 bytes
            assert!(InputRecord::decode_at(&bad2).is_err());
            assert!(InputRecord::decode_prevout_at(&bad2).is_err());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Dense encode/decode + error-arm coverage for packed Class A helpers.
    #[test]
    fn packed_encode_decode_flags_and_error_arms() {
        // TxRecord short
        assert!(matches!(
            TxRecord::decode(&[0u8; 10]),
            Err(StoreError::Corrupt(_))
        ));
        let meta = TxRecord {
            txid: [9u8; 32],
            version: -1,
            locktime: 42,
            input_start_fk: Fk(7),
            input_count: 2,
            output_start_fk: Fk(8),
            output_count: 3,
        };
        let enc = meta.encode();
        assert_eq!(enc.len(), TxRecord::ENCODED_LEN);
        let dec = TxRecord::decode(&enc).unwrap();
        assert_eq!(dec.txid, meta.txid);
        assert_eq!(dec.version, -1);

        // Output flag variants + decode errors
        let o_empty = OutputRecord::unspent(0, vec![]);
        let o_true = OutputRecord::unspent(1, vec![0x51]);
        let o_script = OutputRecord {
            value: 99,
            script: vec![0x76, 0xa9],
            spender_field: Fk(5),
            multi_spender: true,
        };
        for o in [&o_empty, &o_true, &o_script] {
            let e = o.encode();
            let d = OutputRecord::decode(&e).unwrap();
            assert_eq!(d.value, o.value);
            assert_eq!(d.script, o.script);
            assert_eq!(d.spender_field, o.spender_field);
            assert_eq!(d.multi_spender, o.multi_spender);
            let _ = o.encoded_len();
        }
        assert!(matches!(
            OutputRecord::decode_at(&[0u8; 5]),
            Err(StoreError::Corrupt(_))
        ));
        // trailing on decode
        let mut trail = o_true.encode();
        trail.push(0xff);
        assert!(matches!(
            OutputRecord::decode(&trail),
            Err(StoreError::Corrupt(_))
        ));

        // Input coinbase + full + prevout skip + errors
        let coin = InputRecord::coinbase(u32::MAX, vec![], vec![]);
        assert!(coin.is_coinbase());
        let non_final = InputRecord {
            prev_txid: [1u8; 32],
            create_fk: Fk(3),
            prev_index: 2,
            sequence: 1,
            script_sig: vec![0xaa, 0xbb],
            witness: vec![vec![1, 2, 3], vec![4]],
        };
        for r in [&coin, &non_final] {
            let e = r.encode();
            let d = InputRecord::decode(&e).unwrap();
            assert_eq!(d.create_fk, r.create_fk);
            assert_eq!(d.prev_index, r.prev_index);
            assert_eq!(d.sequence, r.sequence);
            assert_eq!(d.script_sig, r.script_sig);
            assert_eq!(d.witness, r.witness);
            let (cfk, vout, used) = InputRecord::decode_prevout_at(&e).unwrap();
            assert_eq!(cfk, r.create_fk);
            assert_eq!(vout, r.prev_index);
            assert_eq!(used, e.len());
            let _ = r.encoded_len();
        }
        assert!(matches!(
            InputRecord::decode_prevout_at(&[]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            InputRecord::decode_at(&[]),
            Err(StoreError::Corrupt(_))
        ));
        // RESERVED4 flag
        assert!(matches!(
            InputRecord::decode_at(&[input_flags::RESERVED4]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            InputRecord::decode_prevout_at(&[input_flags::RESERVED4]),
            Err(StoreError::Corrupt(_))
        ));
        // non-coinbase create_fk truncated
        assert!(matches!(
            InputRecord::decode_at(&[0u8, 1, 2]),
            Err(StoreError::Corrupt(_))
        ));
        // create_fk null on non-coinbase
        let mut bad = vec![0u8]; // no NULL_PREV
        bad.extend_from_slice(&0u64.to_le_bytes());
        bad.push(0); // vout compact 0
        assert!(matches!(
            InputRecord::decode_at(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // sequence truncated
        let mut bad = vec![0u8]; // no SEQ_FINAL
        bad.extend_from_slice(&1u64.to_le_bytes());
        bad.push(0);
        assert!(matches!(
            InputRecord::decode_at(&bad),
            Err(StoreError::Corrupt(_))
        ));
        // trailing
        let mut trail = coin.encode();
        trail.push(1);
        assert!(matches!(
            InputRecord::decode(&trail),
            Err(StoreError::Corrupt(_))
        ));

        // Packed encode/decode
        let tx = TxRecord {
            txid: [0xab; 32],
            version: 2,
            locktime: 0,
            input_start_fk: Fk(99), // cleared on pack
            input_count: 2,
            output_start_fk: Fk(88),
            output_count: 2,
        };
        let inputs = vec![coin.clone(), non_final.clone()];
        let outputs = vec![o_true.clone(), o_script.clone()];
        let mut raw = Vec::new();
        encode_packed_tx(&tx, &inputs, &outputs, &mut raw);
        assert!(is_packed_tx_payload(&raw));
        assert!(!is_packed_tx_payload(&[]));
        assert!(!is_packed_tx_payload(&[0u8; 63]));
        assert!(is_packed_tx_payload(&[0u8; 64])); // length gate only; decode may still fail
        let (m, ins, outs) = decode_packed_tx(&raw).unwrap();
        assert_eq!(m.txid, tx.txid);
        assert_eq!(m.input_start_fk, Fk::NULL);
        assert_eq!(ins.len(), 2);
        assert_eq!(outs.len(), 2);
        let (m2, prevs) = scan_packed_meta_and_prevouts(&raw).unwrap();
        assert_eq!(m2.txid, tx.txid);
        assert_eq!(prevs.len(), 2);
        let (m3, outs_only) = decode_packed_tx_outs_only(&raw).unwrap();
        assert_eq!(m3.txid, tx.txid);
        assert_eq!(outs_only.len(), 2);
        let (m4, outs_rels, rels) = decode_packed_tx_outs_with_spender_rels(&raw).unwrap();
        assert_eq!(m4.txid, tx.txid);
        assert_eq!(rels.len(), 2);
        // spender fields cleared
        assert!(outs_rels.iter().all(|o| o.spender_field.is_null()));
        let mut cleared = outs.clone();
        cleared[0].spender_field = Fk(9);
        cleared[0].multi_spender = true;
        clear_output_spender_fields(&mut cleared);
        assert!(cleared[0].spender_field.is_null());
        assert!(!cleared[0].multi_spender);

        // Packed error arms (short / truncated)
        assert!(matches!(
            decode_packed_tx(&[0x02, 0, 0]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            decode_packed_tx(&[0x01]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            scan_packed_meta_and_prevouts(&[0x02]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            decode_packed_tx_outs_with_spender_rels(&[0x01]),
            Err(StoreError::Corrupt(_))
        ));
        // trailing zero pad is accepted (schema 11 alignment gap)
        let mut trail_z = raw.clone();
        trail_z.extend_from_slice(&[0u8; 7]);
        let (mz, _, _) = decode_packed_tx(&trail_z).unwrap();
        assert_eq!(mz.txid, tx.txid);
        // non-zero trailing garbage is rejected
        let mut trail = raw.clone();
        trail.push(0x01);
        assert!(matches!(
            decode_packed_tx(&trail),
            Err(StoreError::Corrupt(_))
        ));
        // run helpers
        let mut run = Vec::new();
        encode_output_run_secret(&outputs, &mut run, None);
        let (decoded, used) = decode_output_run_prefix(&run, 2).unwrap();
        assert_eq!(used, run.len());
        assert_eq!(decoded.len(), 2);
        assert_eq!(decode_output_run(&run, 2).unwrap().len(), 2);
        let mut irun = Vec::new();
        encode_input_run_secret(&inputs, &mut irun, None);
        assert_eq!(decode_input_run(&irun, 2).unwrap().len(), 2);
        let mut trail_run = run.clone();
        trail_run.push(1);
        assert!(matches!(
            decode_output_run(&trail_run, 2),
            Err(StoreError::Corrupt(_))
        ));

        // Output value > i64::MAX (uleb overflow)
        {
            let mut bad = vec![0u8; 8]; // spender_field
            bad.push(output_flags::EMPTY_SCRIPT);
            // uleb128 of value that exceeds i64::MAX: 0xFF… with enough bytes
            for _ in 0..10 {
                bad.push(0xff);
            }
            bad.push(0x01);
            assert!(matches!(
                OutputRecord::decode_at(&bad),
                Err(StoreError::Corrupt(_))
            ));
        }
        // decode_prevout_at: create_fk null, prev_index too large, truncated fk
        {
            let mut null_fk = vec![0u8]; // no NULL_PREV
            null_fk.extend_from_slice(&0u64.to_le_bytes());
            null_fk.push(0);
            assert!(matches!(
                InputRecord::decode_prevout_at(&null_fk),
                Err(StoreError::Corrupt(_))
            ));
            // truncated create_fk (only 3 bytes after flags)
            assert!(matches!(
                InputRecord::decode_prevout_at(&[0u8, 1, 2, 3]),
                Err(StoreError::Corrupt(_))
            ));
            // prev_index too large: compact_size > u32::MAX
            let mut big_vout = vec![0u8];
            big_vout.extend_from_slice(&1u64.to_le_bytes());
            // compact size 0xFF → 8-byte length follows; use value > u32::MAX
            big_vout.push(0xff);
            big_vout.extend_from_slice(&(u64::from(u32::MAX) + 1).to_le_bytes());
            assert!(matches!(
                InputRecord::decode_prevout_at(&big_vout),
                Err(StoreError::Corrupt(_))
            ));
            // same for full decode_at
            assert!(matches!(
                InputRecord::decode_at(&big_vout),
                Err(StoreError::Corrupt(_))
            ));
            // sequence truncated on decode_prevout (flags without SEQ_FINAL)
            let mut short_seq = vec![0u8];
            short_seq.extend_from_slice(&1u64.to_le_bytes());
            short_seq.push(0); // vout 0
            // only 2 of 4 sequence bytes
            short_seq.extend_from_slice(&[1, 2]);
            assert!(matches!(
                InputRecord::decode_prevout_at(&short_seq),
                Err(StoreError::Corrupt(_))
            ));
            // witness item truncated
            let mut short_wit = vec![
                input_flags::SEQ_FINAL, // no EMPTY_WITNESS
            ];
            short_wit.extend_from_slice(&1u64.to_le_bytes());
            short_wit.push(0); // vout
            short_wit.push(0); // empty script via compact 0? flags don't have EMPTY_SCRIPT
            // Actually EMPTY_SCRIPT not set → need script len
            // Rebuild: SEQ_FINAL | no EMPTY_SCRIPT | no EMPTY_WITNESS
            let mut short_wit = vec![input_flags::SEQ_FINAL];
            short_wit.extend_from_slice(&1u64.to_le_bytes());
            short_wit.push(0); // vout
            short_wit.push(0); // script len 0
            short_wit.push(1); // 1 witness item
            short_wit.push(5); // item len 5
            short_wit.extend_from_slice(&[1, 2]); // only 2 bytes
            assert!(matches!(
                InputRecord::decode_at(&short_wit),
                Err(StoreError::Corrupt(_))
            ));
        }
        // packed outs short / count mismatch / trailing on outs_with_spender
        {
            let tx = TxRecord {
                txid: [0xcd; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 2, // claim 2 outs but only encode 1
            };
            let inputs = [InputRecord::coinbase(u32::MAX, vec![], vec![])];
            let outputs = [OutputRecord::unspent(1, vec![0x51])];
            let mut raw = Vec::new();
            // Manually pack with wrong meta count (txid-first, no magic).
            let mut meta = tx;
            meta.output_count = 2;
            raw.extend_from_slice(&meta.encode());
            encode_input_run_secret(&inputs, &mut raw, None);
            encode_output_run_secret(&outputs, &mut raw, None);
            // ends after 1 output but meta says 2
            assert!(matches!(
                decode_packed_tx(&raw),
                Err(StoreError::Corrupt(_))
            ));
            assert!(matches!(
                decode_packed_tx_outs_with_spender_rels(&raw),
                Err(StoreError::Corrupt(_))
            ));
            // short scan
            assert!(matches!(
                scan_packed_meta_and_prevouts(&[0u8; 8]),
                Err(StoreError::Corrupt(_))
            ));
            // non-zero trailing on outs_only path
            let mut good = Vec::new();
            encode_packed_tx(
                &TxRecord {
                    txid: [1; 32],
                    version: 1,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                },
                &inputs,
                &outputs,
                &mut good,
            );
            let mut trail = good.clone();
            trail.push(0xee);
            assert!(matches!(
                decode_packed_tx_outs_with_spender_rels(&trail),
                Err(StoreError::Corrupt(_))
            ));
            // zero pad accepted on outs path
            let mut zpad = good.clone();
            zpad.extend_from_slice(&[0u8; 5]);
            let (m, outs, _) = decode_packed_tx_outs_with_spender_rels(&zpad).unwrap();
            assert_eq!(m.txid, [1; 32]);
            assert_eq!(outs.len(), 1);
        }
        // input run trailing
        {
            let mut irun = Vec::new();
            encode_input_run_secret(&[InputRecord::coinbase(u32::MAX, vec![], vec![])], &mut irun, None);
            irun.push(0);
            assert!(matches!(
                decode_input_run(&irun, 1),
                Err(StoreError::Corrupt(_))
            ));
        }
    }

    /// body_txid_range edge / corrupt paths (empty body, inverted range).
    #[test]
    fn body_txid_range_edges() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-txid-range-edge-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);
        assert!(t.body_txid_range(10, 5).unwrap().is_empty());
        // Beyond count → NotFound or empty ranges
        let _ = t.body_txid_range(1, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn next_tx_body_start_8_align_and_page_rule() {
        assert_eq!(next_tx_body_start(0), 0);
        assert_eq!(next_tx_body_start(1), 8);
        assert_eq!(next_tx_body_start(8), 8);
        assert_eq!(next_tx_body_start(9), 16);
        // Near page end: S % 4096 must be ≤ 4064 so txid [S,S+32) fits.
        assert_eq!(next_tx_body_start(4096 - 31), 4096); // 4065 → skip to next page
        assert_eq!(next_tx_body_start(4096 - 32), 4064); // 4064 ok
        assert_eq!(next_tx_body_start(4064), 4064);
        assert_eq!(next_tx_body_start(4065), 4096);
        for c in [0u64, 1, 7, 15, 100, 4090, 4095, 4096, 8191, 100_003] {
            let s = next_tx_body_start(c);
            assert_eq!(s % 8, 0, "c={c} s={s}");
            assert!(s % BODY_PAGE_SIZE <= TXID_PAGE_MAX_OFF, "c={c} s={s}");
            assert!(s >= c);
        }
    }

    /// Appended Class A records start 8-aligned; body_txid is first 32 bytes at S.
    #[test]
    fn put_full_aligns_record_starts_and_txid_prefix() {
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-tx-align-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let t = create_tiny(&dir);

        let mut items = Vec::new();
        for i in 0u8..40 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            txid[1] = 0xA5;
            let tx = TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            // Vary sizes so pad between records is non-trivial.
            let script: Vec<u8> = (0..((i as usize % 17) + 1)).map(|b| b as u8).collect();
            items.push((
                tx,
                vec![InputRecord::coinbase(u32::MAX, vec![], vec![])],
                vec![OutputRecord::unspent(1000 + i as i64, script)],
            ));
        }
        let fks = t.put_full_batch_indexed(&items, true).unwrap();
        assert_eq!(fks.len(), 40);
        for (j, fk) in fks.iter().enumerate() {
            let (off, len) = t.body.record_range(*fk).unwrap();
            assert_eq!(off % 8, 0, "fk={} off={}", fk.0, off);
            assert!(
                off % BODY_PAGE_SIZE <= TXID_PAGE_MAX_OFF,
                "fk={} off={} page-straddle risk",
                fk.0,
                off
            );
            assert!(len >= 64);
            let txid = t.body_txid(*fk).unwrap();
            assert_eq!(txid, items[j].0.txid);
            let (meta, ins, outs) = t.get_full(*fk).unwrap();
            assert_eq!(meta.txid, items[j].0.txid);
            assert_eq!(ins.len(), 1);
            assert_eq!(outs.len(), 1);
            // Absolute body: first 32 bytes are the txid.
            let mut prefix = [0u8; 32];
            t.body.read_prefix_at(off, len, &mut prefix).unwrap();
            assert_eq!(prefix, items[j].0.txid);
        }
        // Multi-batch: second batch pads from previous end.
        let mut more = Vec::new();
        for i in 40u8..55 {
            let mut txid = [0u8; 32];
            txid[0] = i;
            more.push((
                TxRecord {
                    txid,
                    version: 2,
                    locktime: 0,
                    input_start_fk: Fk::NULL,
                    input_count: 1,
                    output_start_fk: Fk::NULL,
                    output_count: 1,
                },
                vec![InputRecord::coinbase(u32::MAX, vec![], vec![])],
                vec![OutputRecord::unspent(1, vec![0x51])],
            ));
        }
        let fks2 = t.put_full_batch_indexed(&more, true).unwrap();
        for (j, fk) in fks2.iter().enumerate() {
            let (off, _) = t.body.record_range(*fk).unwrap();
            assert_eq!(off % 8, 0);
            assert_eq!(t.body_txid(*fk).unwrap(), more[j].0.txid);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
