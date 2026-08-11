use crate::address_head::HeadLayout;
use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::segmented_head::SegmentedTxHead;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;

/// Class A tx row (no wire blob — reconstruct from inputs/outputs + witness).
///
/// On-disk packed bodies (schema **13+**): **meta without leading txid**
/// ([`Self::BODY_META_LEN`]) then inputs/outputs. Identity lives in
/// [`crate::txid_body::TxidBody`]. `txid` is filled in-memory from the sidefile
/// (or caller) after decode. `input_start_fk` / `output_start_fk` are always
/// [`Fk::NULL`] on write.
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
    /// On-disk packed meta length (schema 13+: no leading txid).
    pub const BODY_META_LEN: usize = 4 + 4 + 8 + 4 + 8 + 4; // 32
    /// Full in-memory encode size (txid + body meta); used for estimates only.
    pub const ENCODED_LEN: usize = 32 + Self::BODY_META_LEN;

    /// Encode full record including txid (tests / soft buffers — **not** Class A body).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::ENCODED_LEN);
        out.extend_from_slice(&self.txid);
        self.encode_body_meta_into(out);
    }

    /// Encode body meta only (schema 13 packed Class A payload prefix).
    pub fn encode_body_meta_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::BODY_META_LEN);
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

    /// Decode full record with leading txid (soft / test buffers).
    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < Self::ENCODED_LEN {
            return Err(StoreError::Corrupt("short tx record"));
        }
        let mut rec = Self::decode_body_meta(&buf[32..32 + Self::BODY_META_LEN])?;
        rec.txid = buf[0..32].try_into().unwrap();
        Ok(rec)
    }

    /// Decode packed body meta (schema 13); `txid` left zero for caller fill.
    pub fn decode_body_meta(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < Self::BODY_META_LEN {
            return Err(StoreError::Corrupt("short tx body meta"));
        }
        Ok(Self {
            txid: [0u8; 32],
            version: i32::from_le_bytes(buf[0..4].try_into().unwrap()),
            locktime: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            input_start_fk: Fk(u64::from_le_bytes(buf[8..16].try_into().unwrap())),
            input_count: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
            output_start_fk: Fk(u64::from_le_bytes(buf[20..28].try_into().unwrap())),
            output_count: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
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
        let v = if self.value < 0 {
            0u64
        } else {
            self.value as u64
        };
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

    /// Bytes consumed by one packed output starting at `buf` (no script alloc).
    pub fn skip_at(buf: &[u8]) -> Result<usize, StoreError> {
        if buf.len() < 9 {
            return Err(StoreError::Corrupt("short output record"));
        }
        let flags = buf[8];
        let mut off = 9usize;
        let (_v, n) = read_uleb128(&buf[off..])?;
        off += n;
        if flags & (output_flags::EMPTY_SCRIPT | output_flags::OP_TRUE) == 0 {
            let (slen, n) = read_compact_size(&buf[off..])?;
            off += n;
            let slen = slen as usize;
            if buf.len() < off + slen {
                return Err(StoreError::Corrupt("output script truncated"));
            }
            off += slen;
        }
        Ok(off)
    }

    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        let (rec, used) = Self::decode_at(buf)?;
        if used != buf.len() {
            return Err(StoreError::Corrupt("output trailing bytes"));
        }
        Ok(rec)
    }

    /// Capacity upper bound for encode buffers (not byte-exact).
    pub fn encoded_len(&self) -> usize {
        8 + 1 + 10 + 9 + self.script.len()
    }

    /// Exact on-wire length matching [`Self::encode_into`] (for denserels layout).
    #[inline]
    pub fn encoded_len_exact(&self) -> usize {
        use crate::compact::{compact_size_len, uleb128_len};
        let v = if self.value < 0 {
            0u64
        } else {
            self.value as u64
        };
        let mut n = 8 + 1 + uleb128_len(v);
        if self.script.is_empty() || self.script == [0x51] {
            // EMPTY_SCRIPT / OP_TRUE — no script payload.
        } else {
            n += compact_size_len(self.script.len() as u64) + self.script.len();
        }
        n
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

    /// Capacity upper bound for encode buffers (not byte-exact).
    pub fn encoded_len(&self) -> usize {
        // flags + create_fk(8) + vout + sequence + script + witness (upper bound)
        1 + 8
            + 9
            + 4
            + 9
            + self.script_sig.len()
            + 9
            + self.witness.iter().map(|i| 9 + i.len()).sum::<usize>()
    }

    /// Exact on-wire length matching [`Self::encode_into`] (for denserels layout).
    #[inline]
    pub fn encoded_len_exact(&self) -> usize {
        use crate::compact::compact_size_len;
        let null_prev = self.create_fk.is_null() && self.prev_index == u32::MAX;
        let mut n = 1usize; // flags
        if !null_prev {
            n += 8; // create_fk
            n += compact_size_len(u64::from(self.prev_index));
        }
        if self.sequence != u32::MAX {
            n += 4;
        }
        if !self.script_sig.is_empty() {
            n += compact_size_len(self.script_sig.len() as u64) + self.script_sig.len();
        }
        if !self.witness.is_empty() {
            n += compact_size_len(self.witness.len() as u64);
            for item in &self.witness {
                n += compact_size_len(item.len() as u64) + item.len();
            }
        }
        n
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
            secret.xor_bytes(
                u64::from(wi as u32).saturating_add(1) << 16,
                &mut buf[off..off + ilen],
            );
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

/// OS page size (legacy constant; body packing is 8-byte aligned only in schema 13).
pub const BODY_PAGE_SIZE: u64 = 4096;
/// Deprecated (schema 12 body-txid page rule); kept for tests that name it.
pub const TXID_PAGE_MAX_OFF: u64 = BODY_PAGE_SIZE - 32;

/// Next absolute body offset for a Class A packed record (8-byte aligned).
///
/// Schema 13+: identity is in `txid.body`, so body no longer needs page-straddle
/// avoidance for a leading 32-byte txid.
#[inline]
pub fn next_tx_body_start(cursor: u64) -> u64 {
    cursor.saturating_add(7) & !7u64
}

/// Encode a full Class A tx as one var payload (schema **13+**).
///
/// Layout: `body_meta(32) || input_run || output_run` — **no** leading txid.
/// Identity is stored in `txid.body`. Production put paths use
/// [`encode_packed_tx_with_secret`].
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
    meta.encode_body_meta_into(out);
    encode_input_run_secret(inputs, out, secret);
    encode_output_run_secret(outputs, out, secret);
}

/// Dense relative offsets of each output's start within a packed Class A payload.
///
/// Matches [`decode_packed_tx_with_spender_rels`] denserels. Computed from exact
/// encode layout (`encoded_len_exact`) — no full encode+decode. Secret XOR does
/// not change field lengths. Used at plan finish for prep-ahead pin.
pub fn denserels_from_packed_records(
    tx: &TxRecord,
    inputs: &[InputRecord],
    outputs: &[OutputRecord],
) -> Vec<u32> {
    debug_assert_eq!(inputs.len() as u32, tx.input_count);
    debug_assert_eq!(outputs.len() as u32, tx.output_count);
    // Body meta is fixed-width (no txid); fk NULLs on encode do not change length.
    let mut off = TxRecord::BODY_META_LEN;
    for inp in inputs {
        off = off.saturating_add(inp.encoded_len_exact());
    }
    let mut dens = Vec::with_capacity(outputs.len());
    for out in outputs {
        dens.push(off as u32);
        off = off.saturating_add(out.encoded_len_exact());
    }
    dens
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
/// path) so pin/residency never treat pin-time annotations as durable authority.
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
    if raw.len() < TxRecord::BODY_META_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    // Schema 13: body meta only; txid filled by sidefile on get paths.
    let meta = TxRecord::decode_body_meta(&raw[..TxRecord::BODY_META_LEN])?;
    let mut off = TxRecord::BODY_META_LEN;
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
    if raw.len() < TxRecord::BODY_META_LEN {
        return;
    }
    let mut off = TxRecord::BODY_META_LEN;
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
pub fn scan_packed_meta_and_prevouts(raw: &[u8]) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
    if raw.len() < TxRecord::BODY_META_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode_body_meta(&raw[..TxRecord::BODY_META_LEN])?;
    let mut off = TxRecord::BODY_META_LEN;
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
pub fn decode_packed_tx_outs_only(raw: &[u8]) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
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
    if raw.len() < TxRecord::BODY_META_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode_body_meta(&raw[..TxRecord::BODY_META_LEN])?;
    let mut off = TxRecord::BODY_META_LEN;
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

/// Sparse pin decode: only materialize `need_vouts` scripts + denserel slots.
///
/// Walks the full packed layout (inputs skipped, non-need outs skipped without
/// script alloc). Returns `(meta, live outs as (vout, rec), sparse denserels
/// as (vout, rel))`. `need_vouts` should be sorted unique; empty = all outs
/// (same as full denserels decode).
pub fn decode_packed_tx_need_outs_with_spender_rels_secret(
    raw: &[u8],
    need_vouts: &[u32],
    secret: Option<&crate::store_secret::StoreSecret>,
) -> Result<(TxRecord, Vec<(u32, OutputRecord)>, Vec<(u32, u32)>), StoreError> {
    if raw.len() < TxRecord::BODY_META_LEN {
        return Err(StoreError::Corrupt("short packed Class A tx"));
    }
    let meta = TxRecord::decode_body_meta(&raw[..TxRecord::BODY_META_LEN])?;
    let mut off = TxRecord::BODY_META_LEN;
    for _ in 0..meta.input_count {
        let (_txid, _vout, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
    }
    let n_out = meta.output_count;
    // Empty need → all vouts (full materialize path without a second full decode).
    let take_all = need_vouts.is_empty();
    let mut need_i = 0usize;
    let mut live = Vec::with_capacity(if take_all {
        n_out as usize
    } else {
        need_vouts.len()
    });
    let mut sparse = Vec::with_capacity(live.capacity());
    for vout in 0..n_out {
        if off >= raw.len() {
            return Err(StoreError::Corrupt("packed outputs short"));
        }
        let rel = off as u32;
        let want = take_all || (need_i < need_vouts.len() && need_vouts[need_i] == vout);
        if want {
            let (mut rec, used) = OutputRecord::decode_at(&raw[off..])?;
            off += used;
            rec.spender_field = Fk::NULL;
            rec.multi_spender = false;
            if let Some(sec) = secret {
                if !rec.script.is_empty() && rec.script != [0x51] {
                    sec.xor_bytes(0, &mut rec.script);
                }
            }
            live.push((vout, rec));
            sparse.push((vout, rel));
            if !take_all {
                need_i = need_i.saturating_add(1);
            }
        } else {
            off += OutputRecord::skip_at(&raw[off..])?;
        }
    }
    if !take_all && need_i != need_vouts.len() {
        return Err(StoreError::Corrupt(
            "packed need_vouts missing (vout past output_count)",
        ));
    }
    check_trailing_zero_pad(raw, off)?;
    Ok((meta, live, sparse))
}

/// Strip durable spender annotation from outs (pin / residency content-only).
#[inline]
pub fn clear_output_spender_fields(outs: &mut [OutputRecord]) {
    for o in outs {
        o.spender_field = Fk::NULL;
        o.multi_spender = false;
    }
}

/// True when `raw` looks like a schema-13+ packed Class A payload (body meta).
#[inline]
pub fn is_packed_tx_payload(raw: &[u8]) -> bool {
    raw.len() >= TxRecord::BODY_META_LEN
}

/// Segmented `tx.head` occupancy for IBD size logs.
///
/// Name is historical (`HeadResizeSizeSnapshot`); there is no shadow resize.
/// `shadow_*` fields are always zero (compat with older size-log parsers).
#[derive(Clone, Copy, Debug, Default)]
pub struct HeadResizeSizeSnapshot {
    /// Always false — segment roll is synchronous on insert.
    pub active: bool,
    pub cursor: u64,
    pub class_a_n: u64,
    pub primary_bits: u32,
    pub primary_slots: u64,
    pub primary_entry_b: u8,
    pub primary_occupied: u64,
    /// Logical size of one segment head file (`slots × entry_bytes`).
    pub primary_body_bytes: u64,
    /// Deprecated (always 0) — was mono-head shadow geometry.
    pub shadow_bits: u32,
    pub shadow_slots: u64,
    pub shadow_entry_b: u8,
    pub shadow_occupied: u64,
    pub shadow_body_bytes: u64,
    pub segment_count: u64,
    pub sealed_segments: u64,
}

pub struct TxTable {
    pub(crate) body: VarTable,
    /// Segmented fixed-bits heads + seal-time fuse8.
    pub(crate) head: SegmentedTxHead,
    /// Dense create_fk-ordered txids (schema 13+).
    pub(crate) txids: crate::txid_body::TxidBody,
    /// Datadir secret: keyed head probes + script XOR (schema 12+).
    pub(crate) secret: crate::store_secret::StoreSecret,
}

/// Backend for bulk structural 9-byte spender-meta reads on `tx.body`.
///
/// Selected via `RBITCOIN_SPEND_META` / global `RBITCOIN_IO` (see [`crate::io_backend`]).
/// Body peeks are never mmap'd.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpendMetaBackend {
    /// io_uring pread_batch 9B peeks.
    Uring,
    /// libc pread_batch (no ring).
    Pread,
}

/// Structural-meta backend from env hierarchy.
pub fn spend_meta_backend() -> SpendMetaBackend {
    match crate::io_backend::spend_meta_io_backend() {
        crate::io_backend::ReadIoBackend::Uring => SpendMetaBackend::Uring,
        crate::io_backend::ReadIoBackend::Pread => SpendMetaBackend::Pread,
    }
}

/// Deprecated alias — use [`spend_meta_backend`].
#[inline]
pub fn spend_meta_backend_next() -> SpendMetaBackend {
    spend_meta_backend()
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_head_layout(dir, crate::address_head::default_layout())
    }

    /// Create with an explicit head geometry (tests / recovery).
    pub fn create_with_head_layout(dir: &Path, layout: HeadLayout) -> Result<Self, StoreError> {
        let secret = crate::store_secret::StoreSecret::load_or_create(dir, true)?;
        let layout = HeadLayout::with_entry_bytes(layout.bits, 4)?;
        Ok(Self {
            body: VarTable::create(dir, "tx", TableKind::Tx)?,
            head: SegmentedTxHead::create(dir, layout)?,
            txids: crate::txid_body::TxidBody::create(dir)?,
            secret,
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        let body = VarTable::open(dir, "tx", TableKind::Tx)?;
        let txids = crate::txid_body::TxidBody::open(dir)?;
        let n_bodies = body.count();
        let n_txids = txids.count();
        if n_txids != n_bodies {
            // Incomplete Class A publish is the usual cause: body→idx→count then
            // txid.body append. Crash between them leaves body/idx ahead of
            // identity (or rarely the reverse). Align to the **common prefix**
            // instead of demanding a full reindex for a few-thousand-row skew.
            let n = n_bodies.min(n_txids);
            rbitcoin_log::warn!(
                "store: Class A count skew body/idx={n_bodies} txid.body={n_txids} — \
                 truncating to {n} (incomplete last batch; not full reindex)"
            );
            if n_bodies > n {
                body.truncate_to_count(n)?;
            }
            if n_txids > n {
                txids.truncate_to_count(n)?;
            }
            if body.count() != txids.count() {
                return Err(StoreError::Corrupt(
                    "txid.body count != tx body count after repair (reindex required for schema 13)",
                ));
            }
        }
        let mut need_rebuild = false;
        let head = if !crate::segmented_head::head_meta_exists(dir) {
            need_rebuild = n_bodies > 0;
            if need_rebuild {
                rbitcoin_log::info!(
                    "store: tx.head meta missing with {n_bodies} Class A bodies — rebuild segmented head"
                );
            }
            // Wipe legacy mono head if present so create does not refuse after wipe intent.
            let mono = dir.join("tx.head");
            if mono.is_file() {
                let _ = std::fs::remove_file(&mono);
            }
            SegmentedTxHead::create(dir, crate::address_head::default_layout())?
        } else {
            match SegmentedTxHead::open(dir) {
                Ok(h) => h,
                Err(e) => {
                    if n_bodies > 0 {
                        rbitcoin_log::warn!(
                            "store: segmented tx.head unreadable ({e}) with {n_bodies} Class A \
                             bodies — recreate + rebuild"
                        );
                        crate::segmented_head::wipe_segmented_head_files(dir);
                        need_rebuild = true;
                        SegmentedTxHead::create(dir, crate::address_head::default_layout())?
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        let secret = crate::store_secret::StoreSecret::load_or_create(dir, true)?;
        let t = Self {
            body,
            head,
            txids,
            secret,
        };
        if need_rebuild {
            let bits = t.head_bits();
            let slots = t.head_slots();
            rbitcoin_log::info!(
                "store: tx.head rebuild begin n={n_bodies} bits={bits} slots={slots} (segmented)"
            );
            let inserted = t.rebuild_head_from_bodies(|done, total, ins| {
                if done == total || done % 1_000_000 == 0 {
                    rbitcoin_log::info!(
                        "store: tx.head rebuild progress {done}/{total} inserted={ins}"
                    );
                }
            })?;
            t.head.flush()?;
            rbitcoin_log::info!(
                "store: tx.head rebuild complete inserted={inserted} bodies={} bits={} segs={}",
                t.count(),
                t.head_bits(),
                t.head.segment_count()
            );
        } else {
            // Open segment may have creates from a prior process; rebuild fuse
            // keys from Class A so a later seal never FN pre-restart members.
            t.rebuild_open_segment_fuse_keys()?;
            // Soft-migrate sealed fuse8 v1 (xorf/bincode) → v2 without wiping head.
            t.rewrite_legacy_sealed_fuses()?;
        }
        Ok(t)
    }

    /// Rewrite sealed `.fuse8` files that opened as always-probe (legacy v1).
    ///
    /// Rebuilds fuse keys from Class A `txid.body` for each stale sealed segment and
    /// installs a durable v2 payload. Head OA tables are left intact.
    fn rewrite_legacy_sealed_fuses(&self) -> Result<(), StoreError> {
        let queue = self.head.sealed_fuse_rewrite_queue();
        if queue.is_empty() {
            return Ok(());
        }
        rbitcoin_log::warn!(
            "store: rewriting {} sealed tx.head fuse8 file(s) to v2 (format migration; \
             head data kept)",
            queue.len()
        );
        for (file_id, first_fk, count) in queue {
            if count == 0 {
                continue;
            }
            let last_fk = first_fk.saturating_add(count).saturating_sub(1);
            let n_body = self.count();
            if first_fk == 0 || first_fk > n_body || last_fk > n_body {
                rbitcoin_log::warn!(
                    "store: skip fuse rewrite file_id={file_id} first_fk={first_fk} \
                     count={count} body={n_body} (range past Class A)"
                );
                continue;
            }
            let txids = self.body_txid_range(first_fk, last_fk)?;
            if txids.len() as u64 != count {
                return Err(StoreError::Corrupt(
                    "tx.head fuse rewrite: body range count mismatch",
                ));
            }
            let mut keys: Vec<u64> = txids
                .iter()
                .map(|txid| crate::fuse8_filter::fuse_key_from_mixed(&self.secret.mix_txid(txid)))
                .collect();
            keys.sort_unstable();
            keys.dedup();
            let fuse = crate::fuse8_filter::SealedFuse8::build(&keys)?;
            let path = self.head.fuse_path_for_file_id(file_id);
            fuse.write_to(&path)?;
            self.head.install_sealed_fuse(file_id, fuse)?;
            rbitcoin_log::info!(
                "store: tx.head fuse rewritten v2 file_id={file_id} first_fk={first_fk} \
                 count={count} unique_keys={}",
                keys.len()
            );
        }
        Ok(())
    }

    /// Rebuild open-tail fuse keys from Class A body txids (crash/restart safe).
    fn rebuild_open_segment_fuse_keys(&self) -> Result<(), StoreError> {
        let Some((first_fk, count)) = self.head.open_tail_range() else {
            return Ok(());
        };
        if count == 0 {
            return Ok(());
        }
        // After Class A count repair, open-tail may still list fks past body/txid
        // HWM. `replace_open_keys` requires exact open-tail length — skip rebuild
        // when head led identity (stale fuse keys until next seal path rebuilds).
        let n_body = self.count();
        let last_fk = first_fk.saturating_add(count).saturating_sub(1);
        if first_fk == 0 || first_fk > n_body || last_fk > n_body {
            rbitcoin_log::warn!(
                "store: skip open-tail fuse rebuild (head first={first_fk} count={count} \
                 body={n_body}); head may lead truncated Class A"
            );
            return Ok(());
        }
        let txids = self.body_txid_range(first_fk, last_fk)?;
        if txids.len() as u64 != count {
            return Err(StoreError::Corrupt(
                "tx.head open-tail body range count mismatch",
            ));
        }
        let keys: Vec<u64> = txids
            .iter()
            .map(|txid| crate::fuse8_filter::fuse_key_from_mixed(&self.secret.mix_txid(txid)))
            .collect();
        self.head.replace_open_keys(keys)?;
        rbitcoin_log::info!(
            "store: tx.head open-tail fuse keys rebuilt first_fk={first_fk} count={count}"
        );
        Ok(())
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
    pub fn get_meta_and_prevouts(&self, fk: Fk) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
        let (mut tx, prevs) = self
            .body
            .with_raw(fk, |raw| scan_packed_meta_and_prevouts(raw))?;
        tx.txid = self.txids.get(fk)?;
        Ok((tx, prevs))
    }

    /// Full decode from a known body range (skip idx). Txid left zero (no fk).
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

    pub fn put_batch_indexed(&self, recs: &[TxRecord], index: bool) -> Result<Vec<Fk>, StoreError> {
        if recs.is_empty() {
            return Ok(Vec::new());
        }
        // Bare-meta rows: body meta only + sidefile identity (schema 13).
        let est: usize = recs.len() * (TxRecord::BODY_META_LEN + 16);
        let base = self.body.count();
        let fks = self
            .body
            .put_batch_encode_aligned(recs.len(), est, |i, buf| {
                recs[i].encode_body_meta_into(buf);
            })?;
        let ids: Vec<[u8; 32]> = recs.iter().map(|r| r.txid).collect();
        self.txids.append_batch(base, &ids)?;
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
        let (mut tx, _, _, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, Some(&self.secret))?;
        tx.txid = self.txids.get(fk)?;
        Ok(tx)
    }

    /// Read create identity from **`txid.body`** (schema 13+).
    ///
    /// Thin I/O: one 32-byte sidefile pread — no idx / body.
    pub fn body_txid(&self, fk: Fk) -> Result<[u8; 32], StoreError> {
        use std::time::Instant;
        let t = Instant::now();
        let id = self.txids.get(fk)?;
        crate::head_resolve_stats::add_body(t.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_body_lookups(1);
        Ok(id)
    }

    /// Bulk consecutive create txids `first..=last` (1-based) from `txid.body`.
    pub fn body_txid_range(&self, first: u64, last: u64) -> Result<Vec<[u8; 32]>, StoreError> {
        let out = self.txids.get_range(first, last)?;
        crate::head_resolve_stats::add_body_lookups(out.len() as u64);
        Ok(out)
    }

    /// Access dense identity sidefile (tests / resolve machines).
    pub fn txid_sidefile(&self) -> &crate::txid_body::TxidBody {
        &self.txids
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
    fn packed_output_spender_rels(
        raw: &[u8],
        vouts: &[u32],
    ) -> Result<Vec<(u32, u64)>, StoreError> {
        if raw.len() < TxRecord::BODY_META_LEN {
            return Err(StoreError::Corrupt("short packed tx"));
        }
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        let meta = TxRecord::decode_body_meta(&raw[..TxRecord::BODY_META_LEN])?;
        let mut want: Vec<u32> = vouts.to_vec();
        want.sort_unstable();
        want.dedup();
        let max_v = *want.last().unwrap();
        if max_v >= meta.output_count {
            return Err(StoreError::NotFound);
        }
        let mut off = TxRecord::BODY_META_LEN;
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
    /// Deprecated: body no longer stores leading txid (schema 13).
    /// Prefer [`body_txid`] by create_fk. Offset-only path cannot resolve identity
    /// without scanning idx (avoided here) — returns NotFound.
    pub fn body_txid_at(&self, _offset: u64, _len: u64) -> Result<[u8; 32], StoreError> {
        Err(StoreError::NotFound)
    }

    /// Primary head probe slot for `txid` (sort key for locality-friendly batches).
    #[inline]
    pub fn head_primary_slot(&self, txid: &[u8; 32]) -> u64 {
        let bits = self.head.bits();
        crate::address_head::probe_index(txid, 0, bits)
    }

    /// Probe segmented address head and verify body **txid only**.
    ///
    /// Open segment first, then sealed newest→oldest (fuse-gated). Body-check
    /// order prefers deeper probe slots (newest BIP30-shaped create).
    pub fn get_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        use std::time::Instant;
        let mixed = self.secret.mix_txid(txid);
        let t_probe = Instant::now();
        let cands = self.head.probe_candidates(&mixed)?;
        crate::head_resolve_stats::add_probe(t_probe.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_keys(1);
        crate::head_resolve_stats::add_cands(cands.len() as u64);
        for (i, fk) in cands.into_iter().enumerate() {
            // body_txid increments body_lookups + body_ns.
            if self.body_txid(fk)? == *txid {
                crate::head_resolve_stats::add_hit_rank((i as u64).saturating_add(1));
                return Ok(Some(fk));
            }
            crate::head_resolve_stats::add_miss_peeks(1);
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

    /// Batch head resolve for plan stamp: **txid → (create_fk, body_range)**.
    ///
    /// Short-circuit of the Shape A denserels machine
    /// ([`crate::head_resolve_denserels::resolve_fk_and_range_batch`]): probe →
    /// **per-key depth-first** sidefile identity (io_uring when available) → idx
    /// range on hit. **No** cross-key depth-round batching. Prep denserels-loads
    /// via known `body_range` (skip `tx.idx`).
    ///
    /// BIP30: deepest matching create wins (probe order deepest-first).
    /// Timers: [`crate::head_resolve_stats`] probe / idx / body.
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        crate::head_resolve_denserels::resolve_fk_and_range_batch(self, txids)
    }

    /// Sparse denserels/outs by known body ranges (prep pin after plan stamp).
    ///
    /// Each job is `(create_fk, body_range, known_txid, need_vouts)`.
    /// - **Skips `tx.idx`** (range known).
    /// - **`known_txid`**: RAM identity (plan reverse map / residency); not sidefile.
    /// - **`need_vouts`**: sorted unique; empty = all outs. Only those scripts are
    ///   allocated (N2.1). Full body is still pread (layout denserels).
    ///
    /// Returns `(rows, body_ns, decode_ns)` where each row is
    /// `Some((tx, live (vout,out), sparse denserels (vout,rel)))` (N2.0 timers).
    pub fn get_outs_denserels_by_range_batch(
        &self,
        items: &[(Fk, (u64, u64), [u8; 32], Vec<u32>)],
    ) -> Result<
        (
            Vec<Option<(TxRecord, Vec<(u32, OutputRecord)>, Vec<(u32, u32)>)>>,
            u64, /* body_ns */
            u64, /* decode_ns */
        ),
        StoreError,
    > {
        use crate::idx_body_pipeline::{run_idx_body_pipeline_backend, BodyMode, IdxBodyJob};
        use std::time::Instant;
        if items.is_empty() {
            return Ok((Vec::new(), 0, 0));
        }
        let mut jobs: Vec<IdxBodyJob> = items
            .iter()
            .map(|(fk, range, _txid, _need)| IdxBodyJob::new(fk.get().unwrap_or(0), Some(*range)))
            .collect();
        let t_body = Instant::now();
        run_idx_body_pipeline_backend(
            &self.body,
            &mut jobs,
            BodyMode::OutsDenserels,
            crate::io_backend::pin_io_backend(),
        )?;
        let body_ns = t_body.elapsed().as_nanos() as u64;
        let secret = self.store_secret();
        let t_dec = Instant::now();
        let mut out = Vec::with_capacity(jobs.len());
        for (job, (fk, _range, known_txid, need)) in jobs.into_iter().zip(items.iter()) {
            let _ = fk;
            if !job.ok || job.body.is_empty() {
                out.push(None);
                continue;
            }
            match decode_packed_tx_need_outs_with_spender_rels_secret(&job.body, need, Some(secret))
            {
                Ok((mut tx, live, sparse)) => {
                    tx.txid = *known_txid;
                    out.push(Some((tx, live, sparse)));
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => out.push(None),
                Err(e) => return Err(e),
            }
        }
        let decode_ns = t_dec.elapsed().as_nanos() as u64;
        Ok((out, body_ns, decode_ns))
    }

    /// Shape A archive path: Prefix33 select + **one** denserels body per winner.
    ///
    /// - **Miss cands:** Prefix33 only (cheap identity rejects).
    /// - **Multi-cand winners:** Prefix33 until match, then one OutsDenserels.
    /// - **Single-cand keys:** denserels-only (identity + outs in one full pread).
    ///
    /// Never full-body-probes wrong cands (Shape B). Returns
    /// `(txid, Option<(fk, Option<(tx, outs, denserels)>)>)` in input order —
    /// fk may resolve even when denserels decode fails (stamp still works).
    /// Second value is denserels-wave wall ns (archive `head_dens` timer).
    pub fn get_fk_and_outs_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<
        (
            Vec<(
                [u8; 32],
                Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
            )>,
            u64, /* dens_ns */
        ),
        StoreError,
    > {
        // Fused machine: batch probe → idx → Prefix33 → denserels (uring when available).
        crate::head_resolve_denserels::resolve_fk_and_denserels_batch(self, txids)
    }

    /// Bulk `body_range` for many fks (archive sticky + confirm load).
    ///
    /// **Sorted** walk of `tx.idx` via [`VarTable::record_range_batch`] (FdOnly
    /// pread segments) —
    /// same modality as archive head-resolve idx (not scatter io_uring/pread).
    pub fn body_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.body.record_range_batch(fks)
    }

    /// Bulk full packed decode from known ranges.
    ///
    /// Thin decode wrapper over [`crate::idx_body_pipeline`] (body-only jobs).
    /// Fourth field: dense spender_rels relative to body_off.
    pub fn get_full_batch_at(
        &self,
        ranges: &[(Fk, u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>)>>, StoreError>
    {
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
        for (j, (fk, _, _)) in jobs.into_iter().zip(ranges.iter()) {
            if !j.ok {
                out.push(None);
                continue;
            }
            let mut decoded =
                decode_packed_tx_with_spender_rels_secret(&j.body, Some(&self.secret)).ok();
            if let Some(ref mut d) = decoded {
                if let Ok(tid) = self.txids.get(*fk) {
                    d.0.txid = tid;
                }
            }
            out.push(decoded);
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

    /// Bulk 9-byte spender meta reads at absolute `tx.body` file offsets.
    ///
    /// Returns `(spender_field, flags)` — multi = `flags & MULTI_SPENDER`.
    /// Backend from [`spend_meta_backend`] / `RBITCOIN_SPEND_META` /
    /// global `RBITCOIN_IO` (`uring` \| `pread`). Out-of-range / short → `None`.
    pub fn get_spender_meta_at_abs_batch(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.get_spender_meta_at_abs_batch_backend(abs_offs, spend_meta_backend_next())
    }

    /// Like [`Self::get_spender_meta_at_abs_batch`] with an explicit backend.
    pub fn get_spender_meta_at_abs_batch_backend(
        &self,
        abs_offs: &[u64],
        backend: SpendMetaBackend,
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        if abs_offs.is_empty() {
            return Ok(Vec::new());
        }
        match backend {
            SpendMetaBackend::Uring => match self.get_spender_meta_at_abs_batch_uring(abs_offs) {
                Ok(v) => Ok(v),
                Err(e) => {
                    rbitcoin_log::debug!(
                        "store: structural meta uring failed ({e}); pread fallback"
                    );
                    self.get_spender_meta_at_abs_batch_pread(abs_offs)
                }
            },
            SpendMetaBackend::Pread => self.get_spender_meta_at_abs_batch_pread(abs_offs),
        }
    }

    /// io_uring pread_batch 9B peeks.
    fn get_spender_meta_at_abs_batch_uring(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.get_spender_meta_at_abs_batch_fd(abs_offs, crate::io_backend::ReadIoBackend::Uring)
    }

    /// libc pread_batch 9B peeks (no ring).
    fn get_spender_meta_at_abs_batch_pread(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.get_spender_meta_at_abs_batch_fd(abs_offs, crate::io_backend::ReadIoBackend::Pread)
    }

    fn get_spender_meta_at_abs_batch_fd(
        &self,
        abs_offs: &[u64],
        backend: crate::io_backend::ReadIoBackend,
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        use crate::bulk_io::{self, ReadOp};
        const META_LEN: usize = 9;
        let body_fd = self.body.body_read_fd();
        let body_pub = self.body.body_published_len();
        let body_path = self.body.body_file_path();

        let mut bufs: Vec<[u8; META_LEN]> = vec![[0u8; META_LEN]; abs_offs.len()];
        let mut submitted: Vec<usize> = Vec::with_capacity(abs_offs.len());
        for (i, &off) in abs_offs.iter().enumerate() {
            let end = off.saturating_add(META_LEN as u64);
            if end > body_pub {
                continue;
            }
            submitted.push(i);
        }
        if submitted.is_empty() {
            return Ok(vec![None; abs_offs.len()]);
        }

        // SAFETY: each bufs[i] is distinct; submitted indices unique.
        let mut ops: Vec<ReadOp<'_>> = Vec::with_capacity(submitted.len());
        for &i in &submitted {
            let ptr = bufs[i].as_mut_ptr();
            let slice = unsafe { std::slice::from_raw_parts_mut(ptr, META_LEN) };
            ops.push(ReadOp {
                fd: body_fd,
                offset: abs_offs[i],
                buf: slice,
                result: i32::MIN,
                // Confirm write-stage meta: same pages as load pin — do not DONTCACHE.
                dontcache: crate::dontcache_policy::body_read_confirm(),
            });
        }
        bulk_io::pread_batch_backend(&mut ops, backend);

        let mut out: Vec<Option<(Fk, u8)>> = vec![None; abs_offs.len()];
        for (ro, &i) in ops.iter().zip(submitted.iter()) {
            if ro.result < 0 {
                return Err(StoreError::io(
                    body_path,
                    std::io::Error::from_raw_os_error(-ro.result),
                ));
            }
            if ro.result as usize != META_LEN {
                continue;
            }
            let b = &bufs[i];
            let field = Fk(u64::from_le_bytes(b[0..8].try_into().unwrap()));
            let flags = b[8];
            out[i] = Some((field, flags));
        }
        Ok(out)
    }

    /// Pure-write spend annotate using structural-known meta (no body pread).
    ///
    /// `known[i]` is `(field, flags)` at `abs_edges[i].0` from structural spentness.
    /// Backend: `mmap` (map store) or `uring` (pwrite-only). Returns cold edges
    /// (OOB) — production callers must treat non-empty as hard error.
    pub fn put_spend_batch_by_abs_meta_known(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        abs_edges: &[(u64, Fk, u32, Fk)],
        known: &[(Fk, u8)],
        backend: crate::spend_annotate_uring::SpendAnnBackend,
    ) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
        crate::spend_annotate_uring::put_spend_batch_by_abs_meta_known(
            self, spenders, abs_edges, known, backend,
        )
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
        self.body.with_bytes_at(body_off, body_len, |raw| {
            Self::spender_meta_from_raw(raw, vout)
        })
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
        let (mut tx, ins, outs, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, Some(&self.secret))?;
        tx.txid = self.txids.get(fk)?;
        Ok((tx, ins, outs))
    }

    /// Meta + outputs only (one body IO; skips input materialization).
    pub fn get_meta_and_outputs(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        let raw = self.body.get_raw(fk)?;
        let (mut tx, outs, _) =
            decode_packed_tx_outs_with_spender_rels_secret(&raw, Some(&self.secret))?;
        tx.txid = self.txids.get(fk)?;
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
                16 + TxRecord::BODY_META_LEN
                    + ins.iter().map(|i| i.encoded_len()).sum::<usize>()
                    + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let base = self.body.count();
        let fks = self
            .body
            .put_batch_encode_aligned(items.len(), est, |i, buf| {
                let (tx, ins, outs) = &items[i];
                // Schema 13: XOR scripts at rest; body meta without leading txid.
                encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
            })?;
        // Identity sidefile (same create_fk order as body append).
        let ids: Vec<[u8; 32]> = items.iter().map(|(tx, _, _)| tx.txid).collect();
        self.txids.append_batch(base, &ids)?;
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

    /// Like [`Self::put_full_batch_indexed`], but outs live in a shared pin Arc
    /// (tx + outs + denserels). Encode borrows pin fields — no outs deep clone.
    pub fn put_full_batch_from_pins(
        &self,
        items: &[(
            std::sync::Arc<(TxRecord, Vec<OutputRecord>, Vec<u32>)>,
            Vec<InputRecord>,
        )],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let est: usize = items
            .iter()
            .map(|(pin, ins)| {
                let (_tx, outs, _dens) = pin.as_ref();
                16 + TxRecord::BODY_META_LEN
                    + ins.iter().map(|i| i.encoded_len()).sum::<usize>()
                    + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let base = self.body.count();
        let fks = self
            .body
            .put_batch_encode_aligned(items.len(), est, |i, buf| {
                let (pin, ins) = &items[i];
                let (tx, outs, _dens) = pin.as_ref();
                encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
            })?;
        let ids: Vec<[u8; 32]> = items.iter().map(|(pin, _)| pin.0.txid).collect();
        self.txids.append_batch(base, &ids)?;
        if index {
            let heads: Vec<([u8; 32], Fk)> = items
                .iter()
                .zip(fks.iter())
                .map(|((pin, _), fk)| (pin.0.txid, *fk))
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
        // probe_candidates already open-first then sealed newest→oldest, deep-first within.
        let cands = self.head.probe_candidates(&mixed)?;
        for fk in cands {
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
        let mut by_vout: crate::U32Map<(u64, bool, Fk)> =
            crate::U32Map::with_capacity_and_hasher(metas.len(), Default::default());
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
    pub fn backfill_head(&self, on_progress: impl FnMut(u64, u64, u64)) -> Result<u64, StoreError> {
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
        let read_batch: u64 = 65_536;
        let write_chunk: usize = 65_536;
        const PROGRESS_EVERY: u64 = 50_000;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(write_chunk);
        let mut last_progress = 0u64;
        let mut cur = 1u64;
        while cur <= n {
            let end = (cur + read_batch - 1).min(n);
            let txids = self.body_txid_range(cur, end)?;
            for (i, txid) in txids.into_iter().enumerate() {
                let id = cur + i as u64;
                let fk = Fk(id);
                if !force_all {
                    let mixed = self.secret.mix_txid(&txid);
                    let present = self
                        .head
                        .probe_candidates(&mixed)?
                        .iter()
                        .any(|c| c.0 == fk.0);
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

    pub fn head_occupied(&self) -> u64 {
        self.head.occupied()
    }

    pub fn head_bits(&self) -> u32 {
        self.head.bits()
    }

    pub fn head_slots(&self) -> u64 {
        self.head.slots()
    }

    pub fn head_entry_bytes(&self) -> u8 {
        self.head.entry_bytes()
    }

    pub fn head_reserve_additional(&self, _additional: u64) -> Result<(), StoreError> {
        Ok(())
    }

    pub fn head_segment_count(&self) -> usize {
        self.head.segment_count()
    }

    /// Insert txid→fk into the segmented head (mixes keys; may seal/roll).
    ///
    /// Splits the batch so each open segment respects
    /// `MIN(body soft span, 80% head slots)` — soft-span is measured from the
    /// **open segment's first_fk** (not only the first segment in the store).
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mixed: Vec<([u8; 32], Fk)> = entries
            .iter()
            .map(|(txid, fk)| (self.secret.mix_txid(txid), *fk))
            .collect();
        let soft = SegmentedTxHead::soft_span_bytes();
        let mut i = 0usize;
        while i < mixed.len() {
            // Seal open tail first if it already soft-overflows for the next create.
            let force_roll = self.open_soft_span_exceeded(mixed[i].1 .0)?;
            // After an optional seal, the open first_fk for this wave is either
            // the existing open first_fk or the first fk of this sub-batch.
            let wave_first = if force_roll {
                mixed[i].1 .0
            } else {
                self.head
                    .open_tail_range()
                    .map(|(f, _)| f)
                    .unwrap_or(mixed[i].1 .0)
            };
            // Take consecutive entries while body span from wave_first stays ≤ soft.
            let mut j = i;
            while j < mixed.len() {
                if j > i && self.body_span_bytes(wave_first, mixed[j].1 .0)? > soft {
                    break;
                }
                j += 1;
            }
            if j == i {
                j = i + 1; // always make progress (single oversized create)
            }
            self.head.insert_many(&mixed[i..j], force_roll)?;
            i = j;
        }
        Ok(())
    }

    /// True when open segment body span from `open.first_fk` to `next_fk` exceeds soft span.
    fn open_soft_span_exceeded(&self, next_fk: u64) -> Result<bool, StoreError> {
        let soft = SegmentedTxHead::soft_span_bytes();
        let Some((first_fk, count)) = self.head.open_tail_range() else {
            return Ok(false);
        };
        if count == 0 {
            return Ok(false);
        }
        if next_fk < first_fk {
            return Ok(false);
        }
        Ok(self.body_span_bytes(first_fk, next_fk)? > soft)
    }

    fn body_span_bytes(&self, first_fk: u64, last_fk: u64) -> Result<u64, StoreError> {
        if first_fk == 0 || last_fk < first_fk {
            return Ok(0);
        }
        let (off0, _) = self.body.record_range(Fk(first_fk))?;
        let (off1, len1) = self.body.record_range(Fk(last_fk))?;
        Ok(off1.saturating_add(len1).saturating_sub(off0))
    }

    pub fn head_insert_many_sole(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        self.head_insert_many(entries)
    }

    /// Always false — mono-head resize removed (segment roll is synchronous on insert).
    pub fn head_resize_in_progress(&self) -> bool {
        false
    }

    pub fn head_resize_size_snapshot(&self) -> HeadResizeSizeSnapshot {
        let n = self.count();
        let bits = self.head.bits();
        let slots = self.head.slots();
        let occ = self.head.occupied();
        let body_bytes = slots.saturating_mul(u64::from(self.head.entry_bytes()));
        HeadResizeSizeSnapshot {
            active: false,
            cursor: 0,
            class_a_n: n,
            primary_bits: bits,
            primary_slots: slots,
            primary_entry_b: self.head.entry_bytes(),
            primary_occupied: occ,
            primary_body_bytes: body_bytes,
            shadow_bits: 0,
            shadow_slots: 0,
            shadow_entry_b: 0,
            shadow_occupied: 0,
            shadow_body_bytes: 0,
            segment_count: self.head.segment_count() as u64,
            sealed_segments: self.head.sealed_segment_count() as u64,
        }
    }

    /// Flush segmented heads only.
    pub fn flush_head(&self) -> Result<(), StoreError> {
        self.head.flush()
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
mod tests;
