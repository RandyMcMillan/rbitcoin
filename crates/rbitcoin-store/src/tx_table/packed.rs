//! Packed tx body encode/decode (schema Class A payload).

use super::*;

/// Encode a per-tx output run (concat of compact outputs; count lives on TxRecord).
pub(super) fn encode_output_run_secret(
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
pub(super) fn encode_input_run_secret(
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
pub(super) fn xor_script_regions_in_input(
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
pub(super) fn xor_script_region_in_output(
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
pub(super) fn check_trailing_zero_pad(raw: &[u8], logical_end: usize) -> Result<(), StoreError> {
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
pub(super) fn deobfuscate_decoded_inputs_outputs(
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
