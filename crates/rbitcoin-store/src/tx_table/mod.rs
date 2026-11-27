use crate::address_head::HeadLayout;
use crate::compact::{
    amount_exp_mantissa, decode_output_amount, decode_script_kind_v17, encode_script_kind_v17,
    input_flags, output_flags, script_kind_v17_disk_used, split_output_flags,
};
use crate::error::StoreError;
use crate::hashhead::HeadOpenOpts;
use crate::segmented_head::SegmentedTxHead;
use crate::var_table::VarTable;
use rbitcoin_primitives::{le_i32, read_uleb128, write_uleb128, Fk, TableKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Host RAM budget per parallel `tx.head` rebuild worker (not SH extract's 1.5 GiB).
/// BDZ peel scratch + keys + g at the default 2²⁵ seal is ≈1 GiB peak.
pub const TX_HEAD_REBUILD_WORKER_FREE_RAM_BYTES: u64 = 1024 * 1024 * 1024;

/// Max coalesced `txout.body` pread for SH Class A collect. Sequential libc
/// pread (not TLS uring); 16 MiB matches Class A locality.
pub const SCRIPT_HASH_COLLECT_SPAN: u64 = 16 * 1024 * 1024;

/// Stamp short-circuit row: txid → optional (create fk, txout body range).
pub type TxidFkRange = ([u8; 32], Option<(Fk, crate::create_loc::CreateLocPair)>);
/// Sparse denserels-load row: meta + live outs + spender rels.
pub type SparseOutsRow = (TxRecord, Vec<(u32, OutputRecord)>, Vec<(u32, u32)>);
/// Full Class A body: meta + inputs + outputs.
pub type PackedTx = (TxRecord, Vec<InputRecord>, Vec<OutputRecord>);
/// Packed txout decode including spender rels.
pub type PackedTxRels = (TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>);
/// Prep denserels job: create fk, body range, known txid, loc n_out, need-vouts.
pub type OutsByRangeJob = (Fk, (u64, u64), [u8; 32], u32, Vec<u32>);
/// `(rows, body_ns, decode_ns, io)` from [`TxTable::get_outs_by_range_batch`].
pub type OutsByRangeOut = (
    Vec<Option<SparseOutsRow>>,
    u64,
    u64,
    crate::idx_body_pipeline::IdxBodyIoStats,
);
/// `spent.body` slot: spender fk + flags + vin.
pub type SpenderSlot = (Fk, u8, u32);
/// Slots at each asked offset, and what the `spent.body` peeks read.
pub type SpendMetaRead = (Vec<Option<SpenderSlot>>, crate::idx_body_pipeline::PeekIo);

pub fn parse_rebuild_seal_bits(raw: Option<&str>) -> u32 {
    raw.and_then(|s| s.parse::<u32>().ok())
        .map(|b| b.clamp(6, 26))
        .unwrap_or(25)
}

pub fn parse_rebuild_workers(raw: Option<&str>) -> Option<usize> {
    raw.and_then(|s| s.parse::<usize>().ok())
        .map(|n| n.clamp(1, 256))
}

pub fn tx_head_rebuild_workers_for_free_ram(cpus: usize, free_bytes: u64) -> usize {
    crate::sorted_run::workers_for_free_ram(cpus, free_bytes, TX_HEAD_REBUILD_WORKER_FREE_RAM_BYTES)
}

/// Class A tx row (no wire blob — reconstruct from txout + seqsigwit).
///
/// On-disk `txout.body` (schema **17**): thin LAYOUT17 meta then outputs (no spender).
/// Identity lives in [`crate::txid_body::TxidBody`]. `txid` is filled in-memory
/// from the sidefile (or caller) after decode. Body meta stores version,
/// locktime, and leftover schema-24 `n_in` only.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TxRecord {
    pub txid: [u8; 32],
    pub version: i32,
    pub locktime: u32,
    pub input_count: u32,
    pub output_count: u32,
}

impl TxRecord {
    /// Upper bound for buffer estimates (schema-15 16 B; v17 typical is 3).
    pub const BODY_META_LEN: usize = 4 + 4 + 4 + 4;
    /// Full in-memory encode size (txid + body meta); used for estimates only.
    pub const ENCODED_LEN: usize = 32 + Self::BODY_META_LEN;

    /// Encode full record including txid (tests / soft buffers — **not** Class A body).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::ENCODED_LEN);
        out.extend_from_slice(&self.txid);
        self.encode_body_meta_into(out);
    }

    /// Encode `txout` body meta (schema 17 thin LAYOUT17).
    pub fn encode_body_meta_into(&self, out: &mut Vec<u8>) {
        encode_body_meta_v17(self, out);
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(Self::ENCODED_LEN);
        self.encode_into(&mut out);
        out
    }

    /// Decode full record with leading txid (soft / test buffers).
    pub fn decode(buf: &[u8]) -> Result<Self, StoreError> {
        let Some(txid) = buf.first_chunk::<32>() else {
            return Err(StoreError::Corrupt("short tx record"));
        };
        let (mut rec, _) = Self::decode_body_meta(&buf[32..])?;
        rec.txid = *txid;
        Ok(rec)
    }

    /// Decode schema-17 thin `txout` meta. Returns `(record, bytes consumed)`.
    pub fn decode_body_meta(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        decode_body_meta_v17(buf)
    }
}

/// Schema-17 thin `txout` meta flags (codec only until Class A cutover).
/// Bit 7 must be set so a v1 schema-15 prefix (`01 00 00 00`) cannot decode.
#[doc(hidden)]
pub const BODY_META_V17_LAYOUT17: u8 = 1 << 7;
#[doc(hidden)]
pub const BODY_META_V17_VER_1: u8 = 1 << 0;
#[doc(hidden)]
pub const BODY_META_V17_VER_2: u8 = 1 << 1;
#[doc(hidden)]
pub const BODY_META_V17_VER_3: u8 = 1 << 2;
#[doc(hidden)]
pub const BODY_META_V17_LOCKTIME_ZERO: u8 = 1 << 3;
/// `n_in` lives on `txstat.body`; no uleb `input_count` follows locktime.
pub const BODY_META_V17_N_IN_TXSTAT: u8 = 1 << 4;
#[doc(hidden)]
pub const BODY_META_V17_RESERVED: u8 = 0x60;
#[doc(hidden)]
pub const BODY_META_V17_VER_MASK: u8 =
    BODY_META_V17_VER_1 | BODY_META_V17_VER_2 | BODY_META_V17_VER_3;

/// Encode schema-17 thin meta. New writes set [`BODY_META_V17_N_IN_TXSTAT`]
/// and omit the `input_count` uleb (`n_in` is on `txstat.body`).
pub fn encode_body_meta_v17(rec: &TxRecord, out: &mut Vec<u8>) {
    let mut flags = BODY_META_V17_LAYOUT17 | BODY_META_V17_N_IN_TXSTAT;
    match rec.version {
        1 => flags |= BODY_META_V17_VER_1,
        2 => flags |= BODY_META_V17_VER_2,
        3 => flags |= BODY_META_V17_VER_3,
        _ => {}
    }
    if rec.locktime == 0 {
        flags |= BODY_META_V17_LOCKTIME_ZERO;
    }
    out.push(flags);
    if flags & BODY_META_V17_VER_MASK == 0 {
        out.extend_from_slice(&rec.version.to_le_bytes());
    }
    if rec.locktime != 0 {
        write_uleb128(out, u64::from(rec.locktime));
    }
}

/// Decode schema-17 thin meta. Rejects schema-15 16-byte prefixes (no LAYOUT17 bit).
pub fn decode_body_meta_v17(buf: &[u8]) -> Result<(TxRecord, usize), StoreError> {
    if buf.is_empty() {
        return Err(StoreError::Corrupt("short v17 txout meta"));
    }
    let flags = buf[0];
    if flags & BODY_META_V17_LAYOUT17 == 0 {
        return Err(StoreError::Corrupt(
            "legacy txout meta missing LAYOUT17 bit",
        ));
    }
    if flags & BODY_META_V17_RESERVED != 0 {
        return Err(StoreError::Corrupt("v17 txout meta reserved flags"));
    }
    let ver_bits = flags & BODY_META_V17_VER_MASK;
    if ver_bits.count_ones() > 1 {
        return Err(StoreError::Corrupt("v17 txout meta multiple VER bits"));
    }
    let mut off = 1usize;
    let version = if ver_bits == 0 {
        let Some(raw) = buf[off..].first_chunk::<4>() else {
            return Err(StoreError::Corrupt("short v17 txout version"));
        };
        let v = le_i32!(raw, 0);
        off += 4;
        v
    } else if ver_bits == BODY_META_V17_VER_1 {
        1
    } else if ver_bits == BODY_META_V17_VER_2 {
        2
    } else {
        3
    };
    let locktime = if flags & BODY_META_V17_LOCKTIME_ZERO != 0 {
        0
    } else {
        let (v, n) = read_uleb128(&buf[off..])?;
        if v > u64::from(u32::MAX) {
            return Err(StoreError::Corrupt("v17 locktime overflow"));
        }
        off += n;
        v as u32
    };
    let input_count = if flags & BODY_META_V17_N_IN_TXSTAT != 0 {
        0
    } else {
        let (nin, n1) = read_uleb128(&buf[off..])?;
        if nin > u64::from(u32::MAX) {
            return Err(StoreError::Corrupt("v17 input_count overflow"));
        }
        off += n1;
        nin as u32
    };
    Ok((
        TxRecord {
            txid: [0u8; 32],
            version,
            locktime,
            input_count,
            output_count: 0,
        },
        off,
    ))
}

#[doc(hidden)]
pub fn edges_from_seqsigwit_payload(
    raw: &[u8],
) -> Result<Vec<crate::input::InputEdge>, StoreError> {
    let mut off = 0usize;
    let mut edges = Vec::new();
    while off < raw.len() && raw[off..].iter().any(|&b| b != 0) {
        let (create_fk, prev_index, used) = InputRecord::decode_prevout_at(&raw[off..])?;
        off += used;
        if create_fk.is_null() {
            edges.push(crate::input::InputEdge::coinbase());
        } else {
            edges.push(crate::input::InputEdge {
                parent: create_fk,
                vout: prev_index,
            });
        }
    }
    Ok(edges)
}

/// How many creates to locate per input-backfill step.
#[doc(hidden)]
pub const INPUT_BACKFILL_FKS: u64 = 16_384;
/// Coalesced `seqsigwit.body` pread. Same bound as [`SCRIPT_HASH_COLLECT_SPAN`].
#[doc(hidden)]
pub const INPUT_BACKFILL_SPAN: u64 = SCRIPT_HASH_COLLECT_SPAN;

#[doc(hidden)]
pub fn backfill_chunk_end(id: u64, n: u64) -> u64 {
    (id + INPUT_BACKFILL_FKS - 1).min(n)
}

#[doc(hidden)]
pub fn backfill_progress_due(end: u64, id: u64) -> bool {
    end / 1_000_000 != (id - 1) / 1_000_000
}

#[doc(hidden)]
pub fn unstamped_tail(n_bodies: u64, have: u64) -> u64 {
    n_bodies.saturating_sub(have)
}

#[derive(Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum InputOpenTail {
    Backfill { from: u64 },
    Unstamped { n: u64 },
    Ahead,
    Ready,
}

#[doc(hidden)]
pub fn input_open_tail(input_count: u64, n_bodies: u64, seq_count: u64) -> InputOpenTail {
    if n_bodies > 0 && seq_count == n_bodies && input_count < n_bodies {
        InputOpenTail::Backfill {
            from: input_count + 1,
        }
    } else if input_count < n_bodies {
        InputOpenTail::Unstamped {
            n: unstamped_tail(n_bodies, input_count),
        }
    } else if input_count > n_bodies {
        InputOpenTail::Ahead
    } else {
        InputOpenTail::Ready
    }
}

#[doc(hidden)]
pub type ReadSeqsigwitSpan<'a> =
    &'a mut dyn FnMut(u64, u64, &mut Vec<u8>) -> Result<(), StoreError>;

#[doc(hidden)]
pub fn edges_from_seqsigwit_ranges(
    read: ReadSeqsigwitSpan<'_>,
    ranges: &[Option<(u64, u64)>],
    span_max: u64,
    buf: &mut Vec<u8>,
) -> Result<Vec<Vec<crate::input::InputEdge>>, StoreError> {
    let mut out = Vec::with_capacity(ranges.len());
    let mut i = 0usize;
    while i < ranges.len() {
        let Some((start, first_len)) = ranges[i] else {
            return Err(StoreError::Corrupt(
                "invariant: seqsigwit range missing during input backfill",
            ));
        };
        let mut end = start.saturating_add(first_len);
        let mut j = i + 1;
        if first_len <= span_max {
            while j < ranges.len() {
                let Some((off, len)) = ranges[j] else {
                    return Err(StoreError::Corrupt(
                        "invariant: seqsigwit range missing during input backfill",
                    ));
                };
                if off != end {
                    break;
                }
                let next = end.saturating_add(len);
                if next - start > span_max {
                    break;
                }
                end = next;
                j += 1;
            }
        }
        if j <= i {
            return Err(StoreError::Corrupt(
                "invariant: seqsigwit span did not advance",
            ));
        }
        if end > start {
            read(start, end - start, buf)?;
        } else {
            buf.clear();
        }
        if buf.len() as u64 != end.saturating_sub(start) {
            return Err(StoreError::Corrupt(
                "invariant: seqsigwit span short during input backfill",
            ));
        }
        for slot in &ranges[i..j] {
            let (off, len) = slot.ok_or(StoreError::Corrupt(
                "invariant: seqsigwit range missing during input backfill",
            ))?;
            let rel = usize::try_from(off - start)
                .map_err(|_| StoreError::Corrupt("invariant: seqsigwit span offset"))?;
            let n = usize::try_from(len)
                .map_err(|_| StoreError::Corrupt("invariant: seqsigwit span length"))?;
            let at = rel + n;
            if at > buf.len() {
                return Err(StoreError::Corrupt(
                    "invariant: seqsigwit span short during input backfill",
                ));
            }
            out.push(edges_from_seqsigwit_payload(&buf[rel..at])?);
        }
        i = j;
    }
    Ok(out)
}

#[doc(hidden)]
pub fn input_edges(ins: &[InputRecord]) -> Vec<crate::input::InputEdge> {
    ins.iter()
        .map(|inp| match inp.parent() {
            InputParent::Coinbase => crate::input::InputEdge::coinbase(),
            InputParent::Prevout { create_fk, vout } => crate::input::InputEdge {
                parent: create_fk,
                vout,
            },
            // Edges not applied yet. Same bytes the old `(NULL, 0)` pair wrote.
            InputParent::Unstamped => crate::input::InputEdge {
                parent: Fk::NULL,
                vout: 0,
            },
            InputParent::Unlinked { vout } => crate::input::InputEdge {
                parent: Fk::NULL,
                vout,
            },
        })
        .collect()
}

#[doc(hidden)]
pub fn txstat_placeholder() -> crate::txstat::TxStatRow {
    crate::txstat::TxStatRow {
        fee_sat: 0,
        base: 0,
        wit_extra: 0,
    }
}

#[doc(hidden)]
pub fn txstat_placeholders(
    items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
) -> Vec<crate::txstat::TxStatRow> {
    items.iter().map(|_| txstat_placeholder()).collect()
}

/// Class A output (local vout into the txout run).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputRecord {
    /// Satoshis. Decode rejects a mantissa above `i64::MAX` (`output value too large`).
    pub value: u64,
    pub script: Vec<u8>,
    /// Schema v5: sole `spending_tx_fk` if !multi; else head fk into `spent.ovf`.
    pub spender_field: Fk,
    /// When true, `spender_field` is a multi-list head (not a single spending_tx_fk).
    pub multi_spender: bool,
}

impl OutputRecord {
    pub fn unspent(value: u64, script: Vec<u8>) -> Self {
        Self {
            value,
            script,
            spender_field: Fk::NULL,
            multi_spender: false,
        }
    }

    /// Encode `txout` payload (kind nibble + amount exp + ULEB mantissa; no spender).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let _ = self.try_encode_into(out);
    }

    pub fn try_encode_into(&self, out: &mut Vec<u8>) -> Result<(), StoreError> {
        Self::encode_unspent_into(self.value, &self.script, out);
        Ok(())
    }

    /// Same bytes as [`Self::encode_into`] for an unspent `(value, script)` pair.
    pub fn encode_unspent_into(value: u64, script: &[u8], out: &mut Vec<u8>) {
        let flags_at = out.len();
        out.push(0);
        let (exp, mantissa) = amount_exp_mantissa(value);
        write_uleb128(out, mantissa);
        let kind = encode_script_kind_v17(script, out);
        out[flags_at] = kind | (exp << 4);
    }

    /// Capacity upper bound matching [`Self::encoded_len`] without an [`OutputRecord`].
    #[inline]
    pub fn encoded_len_for_script(script_len: usize) -> usize {
        1 + 10 + 9 + script_len
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + 10 + 9 + self.script.len());
        self.encode_into(&mut out);
        out
    }

    /// Decode one `txout` output; spender fields are left null (load from `spent.body`).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        Self::decode_at_secret(buf, None)
    }

    pub fn decode_at_secret(
        buf: &[u8],
        secret: Option<&crate::store_secret::StoreSecret>,
    ) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short output record"));
        }
        let flags = buf[0];
        let (kind, exp) = split_output_flags(flags)?;
        let mut off = 1usize;
        let (value, n) = decode_output_amount(exp, &buf[off..])?;
        off += n;
        let used = script_kind_v17_disk_used(kind, &buf[off..])?;
        let script = if let Some(sec) = secret {
            let mut payload = buf[off..off + used].to_vec();
            packed::xor_script_kind_v17_payload(kind, &mut payload, sec);
            decode_script_kind_v17(kind, &payload)?.0
        } else {
            decode_script_kind_v17(kind, &buf[off..])?.0
        };
        off += used;
        Ok((
            Self {
                value,
                script,
                spender_field: Fk::NULL,
                multi_spender: false,
            },
            off,
        ))
    }

    /// Bytes consumed by one `txout` output starting at `buf` (no script alloc).
    pub fn skip_at(buf: &[u8]) -> Result<usize, StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short output record"));
        }
        let flags = buf[0];
        let (kind, exp) = split_output_flags(flags)?;
        let mut off = 1usize;
        let (_v, n) = decode_output_amount(exp, &buf[off..])?;
        off += n;
        off += script_kind_v17_disk_used(kind, &buf[off..])?;
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
        1 + 10 + 9 + self.script.len()
    }

    /// Exact on-wire length matching [`Self::encode_into`].
    #[inline]
    pub fn encoded_len_exact(&self) -> usize {
        use crate::compact::{
            classify_script, SCRIPT_KIND_V17_OP_RETURN_PUSH, SCRIPT_KIND_V17_RAW,
        };
        use rbitcoin_primitives::{compact_size_len, uleb128_len};
        let (_exp, mantissa) = amount_exp_mantissa(self.value);
        let (kind, payload) = classify_script(&self.script);
        let payload_len = match kind {
            SCRIPT_KIND_V17_RAW | SCRIPT_KIND_V17_OP_RETURN_PUSH => {
                compact_size_len(payload.len() as u64) + payload.len()
            }
            _ => payload.len(),
        };
        1 + uleb128_len(mantissa) + payload_len
    }

    /// Sole-spender slot length in `spent.body`.
    pub const SPENT_SLOT_LEN: usize = 8;
}

#[doc(hidden)]
pub mod packed;
#[doc(hidden)]
pub mod pending_head;
pub use packed::*;
pub use pending_head::PENDING_HEAD_CAP;

#[doc(hidden)]
pub fn span_rec(
    span: &[u8],
    span_off: u64,
    rec_off: u64,
    rec_len: u64,
) -> Result<&[u8], StoreError> {
    let start = rec_off
        .checked_sub(span_off)
        .ok_or(StoreError::Corrupt("span record before span"))?;
    let start = usize::try_from(start).map_err(|_| StoreError::Corrupt("span start"))?;
    let len = usize::try_from(rec_len).map_err(|_| StoreError::Corrupt("span rec len"))?;
    let end = start
        .checked_add(len)
        .ok_or(StoreError::Corrupt("span rec end"))?;
    span.get(start..end)
        .ok_or(StoreError::Corrupt("span record OOB"))
}

#[doc(hidden)]
pub fn pread_two_spans(
    a: &VarTable,
    a_off: u64,
    a_len: u64,
    b: &VarTable,
    b_off: u64,
    b_len: u64,
    parallel: bool,
) -> Result<(Vec<u8>, Vec<u8>), StoreError> {
    if !parallel {
        return Ok((a.pread_span(a_off, a_len)?, b.pread_span(b_off, b_len)?));
    }
    std::thread::scope(|s| {
        let ta = s.spawn(|| a.pread_span(a_off, a_len));
        let tb = s.spawn(|| b.pread_span(b_off, b_len));
        let va = ta
            .join()
            .unwrap_or(Err(StoreError::Corrupt("txout span thread")))?;
        let vb = tb
            .join()
            .unwrap_or(Err(StoreError::Corrupt("seqsigwit span thread")))?;
        Ok((va, vb))
    })
}

/// Read one ring of spend peeks. `members` index `live`, which indexes `orig`.
///
/// Merged groups read into one bounce from the thread's kept pool, as body
/// peeks do, so a repeat wave does not allocate and zero its spans again. A
/// chunk is at most [`crate::uring_session::DEFAULT_ENTRIES`] groups.
#[doc(hidden)]
pub fn read_spend_peek_chunk(
    chunk: &[crate::idx_body_pipeline::SpendSlotPeek],
    live: &[u64],
    orig: &[usize],
    out: &mut [Option<SpenderSlot>],
    body_fd: crate::io_handle::IoHandle,
    body_path: &std::path::Path,
    backend: crate::io_backend::ReadIoBackend,
) -> Result<crate::idx_body_pipeline::PeekIo, StoreError> {
    use crate::bulk_io::{self, ReadOp};
    const META_LEN: usize = OutputRecord::SPENT_SLOT_LEN;
    let mut singles = vec![[0u8; META_LEN]; chunk.len()];
    let mut bounce_at: Vec<Option<std::ops::Range<usize>>> = Vec::with_capacity(chunk.len());
    let mut bounce_need = 0usize;
    for g in chunk {
        if g.members.len() == 1 {
            bounce_at.push(None);
            continue;
        }
        let end = usize::try_from(g.len)
            .ok()
            .and_then(|len| bounce_need.checked_add(len))
            .ok_or(StoreError::Corrupt("spend peek span"))?;
        bounce_at.push(Some(bounce_need..end));
        bounce_need = end;
    }
    let mut bounce = crate::idx_body_pipeline::take_bounce(bounce_need);
    let bounce_base = bounce.as_mut_ptr();
    let mut ops: Vec<ReadOp<'_>> = Vec::with_capacity(chunk.len());
    for ((g, single), at) in chunk.iter().zip(singles.iter_mut()).zip(&bounce_at) {
        let buf: &mut [u8] = match at {
            None => single,
            Some(at) => {
                if at.end > bounce.len() {
                    return Err(StoreError::Corrupt(
                        "invariant: merged spend peek past its bounce",
                    ));
                }
                // SAFETY: `at` lies inside `bounce` (checked) and is this
                // group's range alone; `bounce` is not touched until the
                // reads are done.
                unsafe { std::slice::from_raw_parts_mut(bounce_base.add(at.start), at.len()) }
            }
        };
        ops.push(ReadOp {
            fd: body_fd,
            offset: g.off,
            buf,
            result: i32::MIN,
        });
    }
    let io = crate::idx_body_pipeline::PeekIo {
        sqe_n: ops.len() as u64,
        read_bytes: ops.iter().map(|op| op.buf.len() as u64).sum(),
        kept_bytes: chunk
            .iter()
            .map(|g| (g.members.len() * META_LEN) as u64)
            .sum(),
    };
    bulk_io::pread_batch_backend(&mut ops, backend);
    let results: Vec<i32> = ops.iter().map(|ro| ro.result).collect();
    drop(ops);
    for (((g, single), at), result) in chunk.iter().zip(&singles).zip(&bounce_at).zip(results) {
        if result < 0 {
            return Err(StoreError::io(
                body_path,
                std::io::Error::from_raw_os_error(-result),
            ));
        }
        let raw: &[u8] = match at {
            None => single,
            Some(at) => &bounce[at.clone()],
        };
        let got = result as usize;
        for &m in &g.members {
            let rel = usize::try_from(live[m].saturating_sub(g.off)).unwrap_or(usize::MAX);
            if rel.saturating_add(META_LEN) > got.min(raw.len()) {
                continue;
            }
            let Ok((flags, field, vin)) = decode_spent_slot(&raw[rel..rel + META_LEN]) else {
                continue;
            };
            out[orig[m]] = Some((field, flags, vin));
        }
    }
    crate::idx_body_pipeline::keep_bounce(bounce);
    Ok(io)
}

pub struct TxTable {
    /// `txout.body` — meta + outputs (hot).
    pub body: VarTable,
    /// `seqsigwit.body` — inputs + witness (cold).
    pub seqsigwit: VarTable,
    /// `spent.body` — 8 B × n_out sole-spender slots.
    pub spent: VarTable,
    pub create_loc: crate::create_loc::CreateLoc,
    pub seqsigwit_loc: crate::delta_loc::DeltaLoc,
    /// Segmented fixed-bits heads + seal-time fuse8.
    /// Segmented fixed-bits heads + seal-time fuse8.
    pub head: SegmentedTxHead,
    /// Dense create_fk-ordered txids (schema 13+).
    pub txids: crate::txid_body::TxidBody,
    /// Dense create_fk-ordered confirm-time econ (schema 25).
    pub txstat: crate::txstat::TxStat,
    /// Spender → parent edges (`input.loc` / `input.body`).
    pub input: crate::input::Input,
    /// Datadir secret: keyed head probes + script XOR (schema 12+).
    pub secret: crate::store_secret::StoreSecret,
    /// Unflushed head inserts (write-behind). Readers see published snapshot.
    pub pending_head: pending_head::PendingHeadInserts,
    pub rebuild_seal_bits: u32,
    pub rebuild_workers: usize,
    pub prune_seqsigwit_mode: std::sync::atomic::AtomicBool,
    /// `--datadir-hdd`: Class A body stems are one pwrite wave each.
    pub datadir_hdd: std::sync::atomic::AtomicBool,
    /// Schema-30 spend delta: every spend slot, over the pages folds wrote.
    spend_pages: crate::spend_delta::SpendPageLog,
    /// Held across every Class A sync. One sync at a time, so a sync that
    /// returns 0 because another took the writeback error cannot commit
    /// after that failure.
    class_a_sync: std::sync::Mutex<()>,
    /// Set once a Class A sync or append write failed. The kernel may have
    /// dropped those dirty pages, and RAM may name rows the device does not
    /// hold, so a retry that succeeds proves nothing: every later sync and
    /// append refuses until restart.
    class_a_failed: std::sync::atomic::AtomicBool,
    /// Told once when [`Self::class_a_failed`] latches.
    class_a_on_fail: std::sync::OnceLock<Box<dyn Fn() + Send + Sync>>,
    /// Hot store directory, where [`CLASS_A_SYNCED_NAME`] lives.
    hot_dir: PathBuf,
    /// The value in [`CLASS_A_SYNCED_NAME`]. Written under the latch.
    synced_named: std::sync::atomic::AtomicU64,
    /// Creates whose every Class A row is written. [`Self::count`] moves
    /// when the bodies publish, before `txid.body`, `txstat`, and `input`.
    settled: std::sync::atomic::AtomicU64,
    /// Per-fk packed `txout` decodes for an outs reader
    /// ([`Self::get_meta_and_outputs`], and [`Self::get`] through it), the
    /// decode the mempool and block-proposal pins count. [`Self::get_full`] is
    /// the confirm write stage's decoder and is not counted; span and pin
    /// machines decode raw spans and are not counted. One Relaxed add per
    /// decode. Debug builds only: tests read it, production does not.
    #[cfg(debug_assertions)]
    pub body_decodes: std::sync::atomic::AtomicU64,
    /// Output records materialized from packed bodies (instance stats).
    #[cfg(debug_assertions)]
    pub outputs_decoded: std::sync::atomic::AtomicU64,
    /// `Store::input_edges_span` reads (instance stats).
    #[cfg(debug_assertions)]
    pub input_edge_spans: std::sync::atomic::AtomicU64,
}

/// Structural-meta backend from env hierarchy.
pub fn spend_meta_backend() -> crate::io_backend::ReadIoBackend {
    crate::io_backend::read_io_backend()
}

#[doc(hidden)]
pub fn class_a_body_occupied(dir: &Path, stem: &str) -> bool {
    let p = dir.join(format!("{stem}.body"));
    match std::fs::metadata(&p) {
        Ok(m) => m.len() > crate::file::FILE_HEADER_LEN as u64,
        Err(_) => false,
    }
}

/// Sidecar in the hot store: every create through this count was synced
/// before a durable record named it without a `header_txs` range (a spend
/// commit, a scripthash mark). See [`TxTable::sync_class_a_named`].
pub const CLASS_A_SYNCED_NAME: &str = "class_a.synced";

/// [`CLASS_A_SYNCED_NAME`], or 0 when absent.
pub fn read_class_a_synced(dir: &Path) -> Result<u64, StoreError> {
    let path = dir.join(CLASS_A_SYNCED_NAME);
    match std::fs::read(&path) {
        Ok(b) => b
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| StoreError::Corrupt("class_a.synced size")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(e) => Err(StoreError::io(&path, e)),
    }
}

/// `create` leaves [`crate::file::CREATE_FILE_SLACK`] bytes. Anything past
/// that was a payload, including after a truncate that did not shrink the
/// allocation. Header-only and Class C stores stay at the slack.
fn allocated_past_slack(bodies: &[&VarTable]) -> bool {
    bodies.iter().any(|t| {
        let published = t.body.logical_len();
        let cap = t.body.file_cap.load(std::sync::atomic::Ordering::Acquire);
        cap > published.saturating_add(crate::file::CREATE_FILE_SLACK)
    })
}

/// Refuse to keep fewer than `committed` creates.
///
/// This binary syncs Class A before a durable Class C row names a create,
/// so a fit below that row was left by an older binary or by a device that
/// lost synced writes. Cutting would hand those numbers to later
/// transactions under stale spends and postings. Open checks before each
/// cut that could go below the floor.
pub(crate) fn refuse_below_committed(count: u64, committed: u64) -> Result<(), StoreError> {
    if count >= committed {
        return Ok(());
    }
    rbitcoin_log::error!(
        "store: Class A ends at create {count} and committed rows name {committed} \
         (an older binary, or the device lost synced writes)"
    );
    Err(StoreError::Corrupt(
        "Class A ends below a committed create (an older binary, or the device lost synced \
         writes); wipe the datadir and reindex",
    ))
}

fn open_or_create_create_loc_above(
    dir: &Path,
    floor: u64,
) -> Result<crate::create_loc::CreateLoc, StoreError> {
    if dir.join("create.loc").exists() {
        crate::create_loc::CreateLoc::open_above(dir, floor)
    } else if class_a_body_occupied(dir, "txout") {
        Err(StoreError::Corrupt("invariant: create.loc missing"))
    } else {
        crate::create_loc::CreateLoc::create(dir)
    }
}

fn open_or_create_seqsigwit_loc_above(
    seqsigwit_dir: &Path,
    floor: u64,
) -> Result<crate::delta_loc::DeltaLoc, StoreError> {
    if seqsigwit_dir.join("seqsigwit.loc").exists() {
        crate::delta_loc::DeltaLoc::open_above(seqsigwit_dir, "seqsigwit", floor)
    } else if class_a_body_occupied(seqsigwit_dir, "seqsigwit") {
        Err(StoreError::Corrupt("invariant: seqsigwit.loc missing"))
    } else {
        crate::delta_loc::DeltaLoc::create(seqsigwit_dir, "seqsigwit")
    }
}

#[doc(hidden)]
pub fn unlink_leftover_class_a_idx(dir: &Path) -> Result<(), StoreError> {
    for stem in ["txout", "spent", "seqsigwit"] {
        let p = dir.join(format!("{stem}.idx"));
        if p.exists() {
            if p.is_dir() {
                std::fs::remove_dir_all(&p).map_err(|e| StoreError::io(&p, e))?;
            } else {
                std::fs::remove_file(&p).map_err(|e| StoreError::io(&p, e))?;
            }
            rbitcoin_log::warn!(
                "store: dropping leftover {stem}.idx (schema 22 uses create.loc / seqsigwit.loc)"
            );
        }
    }
    Ok(())
}

#[doc(hidden)]
pub struct ClassASkewStems<'a> {
    pub create_loc: &'a crate::create_loc::CreateLoc,
    pub seqsigwit_loc: &'a crate::delta_loc::DeltaLoc,
    pub body: &'a VarTable,
    pub spent: &'a VarTable,
    pub seqsigwit: &'a VarTable,
    pub txids: &'a crate::txid_body::TxidBody,
    pub txstat: &'a crate::txstat::TxStat,
    pub input: &'a crate::input::Input,
}

#[doc(hidden)]
pub fn class_a_skew_target_count(
    n_loc: u64,
    n_txids: u64,
    n_seqsigwit_loc: u64,
    prune_seqsigwit_mode: bool,
    short_new_layout_input: Option<u64>,
) -> Option<u64> {
    if let Some(n) = short_new_layout_input {
        return Some(n);
    }
    // Count 0 is the finished cut. Min-ing it would drop the chain.
    // A positive locator that is short of the creates is an unfinished
    // commit, and the chain truncates to the rows it still names.
    let witness_cut = prune_seqsigwit_mode && n_seqsigwit_loc == 0;
    if n_txids == n_loc && (witness_cut || n_seqsigwit_loc == n_loc) {
        return None;
    }
    Some(if witness_cut {
        n_loc.min(n_txids)
    } else {
        n_loc.min(n_txids).min(n_seqsigwit_loc)
    })
}

#[doc(hidden)]
pub fn stem_min(n_loc: u64, n_txids: u64, n_seqsigwit_loc: u64, prune_seqsigwit_mode: bool) -> u64 {
    if prune_seqsigwit_mode {
        n_loc.min(n_txids)
    } else {
        n_loc.min(n_txids).min(n_seqsigwit_loc)
    }
}

/// `true` when create `fk` stores its parent edge on `input.body`.
#[doc(hidden)]
pub fn create_prevout_on_inputs(stems: &ClassASkewStems<'_>, fk: Fk) -> Result<bool, StoreError> {
    let Some(Some((off, len))) = stems.seqsigwit_loc.range_batch(&[fk])?.into_iter().next() else {
        return Ok(false);
    };
    if len == 0 {
        return Ok(false);
    }
    let flags = stems.seqsigwit.with_bytes_at(off, 1, |b| Ok(b[0]))?;
    Ok(flags & input_flags::PREV_ON_INPUTS != 0)
}

/// A short `input` that already existed, whose next create is new-layout.
///
/// Legacy inline prevouts stay out of this floor so open can still backfill
/// them. A fresh `Input::create` this open is not a power-loss tail.
#[doc(hidden)]
pub fn short_new_layout_input(
    stems: &ClassASkewStems<'_>,
    n_loc: u64,
    n_txids: u64,
    n_seqsigwit_loc: u64,
    prune_seqsigwit_mode: bool,
    input_existed: bool,
) -> Result<Option<u64>, StoreError> {
    if !input_existed {
        return Ok(None);
    }
    let n_in = stems.input.count();
    let stem = stem_min(n_loc, n_txids, n_seqsigwit_loc, prune_seqsigwit_mode);
    if n_in >= stem || stem == 0 {
        return Ok(None);
    }
    let next = Fk(n_in.saturating_add(1));
    if create_prevout_on_inputs(stems, next)? {
        Ok(Some(n_in))
    } else {
        Ok(None)
    }
}

/// Prune mode with a witness loc that does not cover the creates cannot
/// rebuild a short `input` from `seqsigwit`. The input count is the prefix
/// that landed.
#[doc(hidden)]
pub fn pruned_uncovered_input_floor(
    stems: &ClassASkewStems<'_>,
    n_loc: u64,
    n_txids: u64,
    n_seqsigwit_loc: u64,
    prune_seqsigwit_mode: bool,
    input_existed: bool,
) -> Option<u64> {
    if !prune_seqsigwit_mode || !input_existed {
        return None;
    }
    let stem = stem_min(n_loc, n_txids, n_seqsigwit_loc, true);
    if stem == 0 || n_seqsigwit_loc >= stem {
        return None;
    }
    let n_in = stems.input.count();
    if n_in >= stem {
        return None;
    }
    Some(n_in)
}

#[doc(hidden)]
pub fn class_a_skew_stem_ends(
    stems: &ClassASkewStems<'_>,
    n: u64,
    prune_seqsigwit_mode: bool,
) -> Result<(u64, u64, u64), StoreError> {
    if n == 0 {
        let h = crate::file::FILE_HEADER_LEN as u64;
        return Ok((h, h, h));
    }
    let p = stems
        .create_loc
        .range_batch(&[Fk(n)])?
        .into_iter()
        .next()
        .flatten()
        .ok_or(StoreError::Corrupt("invariant: loc range for truncate"))?;
    let in_end = if prune_seqsigwit_mode {
        crate::file::FILE_HEADER_LEN as u64
    } else {
        let ir = stems
            .seqsigwit_loc
            .range_batch(&[Fk(n)])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::Corrupt(
                "invariant: seqsigwit.loc range for truncate",
            ))?;
        ir.0.saturating_add(ir.1)
    };
    Ok((
        p.txout.0.saturating_add(p.txout.1),
        p.spent.0.saturating_add(p.spent.1),
        in_end,
    ))
}

#[doc(hidden)]
pub fn class_a_skew_apply_truncate(
    stems: &ClassASkewStems<'_>,
    n: u64,
    n_txids: u64,
    tx_end: u64,
    sp_end: u64,
    in_end: u64,
    prune_seqsigwit_mode: bool,
) -> Result<(), StoreError> {
    stems.create_loc.truncate_to_count(n)?;
    if !prune_seqsigwit_mode {
        stems.seqsigwit_loc.truncate_to_count(n)?;
    }
    stems.body.truncate_body_to(n, tx_end)?;
    stems.spent.truncate_body_to(n, sp_end)?;
    if !prune_seqsigwit_mode {
        stems.seqsigwit.truncate_body_to(n, in_end)?;
    }
    if n_txids > n {
        stems.txids.truncate_to_count(n)?;
    }
    if stems.txstat.count() > n {
        stems.txstat.truncate_to_count(n)?;
    }
    if stems.input.count() > n {
        stems.input.truncate_to_count(n)?;
    }
    Ok(())
}

#[doc(hidden)]
pub fn class_a_skew_assert_aligned(
    stems: &ClassASkewStems<'_>,
    prune_seqsigwit_mode: bool,
) -> Result<(), StoreError> {
    if stems.body.count() != stems.txids.count() || stems.create_loc.count() != stems.txids.count()
    {
        return Err(StoreError::Corrupt(
            "Class A stem counts still mismatch after repair (reindex required)",
        ));
    }
    if !prune_seqsigwit_mode && stems.seqsigwit_loc.count() != stems.txids.count() {
        return Err(StoreError::Corrupt(
            "Class A stem counts still mismatch after repair (reindex required)",
        ));
    }
    Ok(())
}

#[doc(hidden)]
///
/// `named` is the highest create a durable record names: a `header_txs`
/// range, or [`CLASS_A_SYNCED_NAME`]. Every such record was written after a
/// Class A sync, so every create through `named` is on the device and every
/// one past it is named by nothing durable. Open keeps exactly `named`
/// creates and cuts the rest without reading it: a torn row can decode (a
/// zeroed input edge is a coinbase edge), so reading proves nothing. A file
/// short of `named` was left by a binary without the barrier, or a device
/// that lost synced writes: open refuses before it cuts a body
/// ([`refuse_below_committed`]). `None` (a table opened alone) keeps the
/// prefix every file covers.
pub fn repair_class_a_count_skew(
    stems: ClassASkewStems<'_>,
    prune_seqsigwit_mode: bool,
    input_existed: bool,
    named: Option<u64>,
) -> Result<bool, StoreError> {
    let committed = named.unwrap_or(0);
    let n_loc = stems.create_loc.count();
    let n_txids = stems.txids.count();
    let n_seqsigwit_loc = stems.seqsigwit_loc.count();
    let stem =
        class_a_skew_target_count(n_loc, n_txids, n_seqsigwit_loc, prune_seqsigwit_mode, None)
            .unwrap_or(n_loc);
    refuse_below_committed(stem, committed)?;
    let stem = named.unwrap_or(stem);
    // A range read decodes its whole locator window up to the count, so a
    // zeroed row past `stem` would fail reads of the creates kept below it.
    if stems.create_loc.count() > stem {
        stems.create_loc.truncate_to_count(stem)?;
    }
    if !prune_seqsigwit_mode && stems.seqsigwit_loc.count() > stem {
        stems.seqsigwit_loc.truncate_to_count(stem)?;
    }
    // Those cuts were above `named`. Nothing below it is cut.
    let covered = body_headers_cover(&stems, stem, prune_seqsigwit_mode)?;
    refuse_below_committed(covered, committed)?;
    // The short-input probe reads the witness row past the input count. Run
    // it only inside the covered prefix, where that row is on disk.
    let short_input = if stems.input.count() < covered {
        match pruned_uncovered_input_floor(
            &stems,
            n_loc,
            n_txids,
            n_seqsigwit_loc,
            prune_seqsigwit_mode,
            input_existed,
        ) {
            Some(n) => Some(n),
            None => short_new_layout_input(
                &stems,
                n_loc,
                n_txids,
                n_seqsigwit_loc,
                prune_seqsigwit_mode,
                input_existed,
            )?,
        }
    } else {
        None
    };
    let skewed = class_a_skew_target_count(
        n_loc,
        n_txids,
        n_seqsigwit_loc,
        prune_seqsigwit_mode,
        short_input,
    );
    let n = skewed.unwrap_or(n_loc).min(covered);
    refuse_below_committed(n, committed)?;
    let skewed = skewed.or((n < n_loc).then_some(n));
    let (tx_end, sp_end, in_end) = class_a_skew_stem_ends(&stems, n, prune_seqsigwit_mode)?;
    // An input stem past the count is that same unpublished tail.
    let ahead = stems.body.body_logical_len() > tx_end
        || stems.spent.body_logical_len() > sp_end
        || (!prune_seqsigwit_mode && stems.seqsigwit.body_logical_len() > in_end)
        || stems.input.count() > n;
    if skewed.is_none() && !ahead {
        return Ok(false);
    }
    let n_input = stems.input.count();
    rbitcoin_log::warn!(
        "store: Class A count skew loc={n_loc} seqsigwit.loc={n_seqsigwit_loc} \
         txid.body={n_txids} input={n_input} — truncating to {n}"
    );
    class_a_skew_apply_truncate(
        &stems,
        n,
        n_txids,
        tx_end,
        sp_end,
        in_end,
        prune_seqsigwit_mode,
    )?;
    class_a_skew_assert_aligned(&stems, prune_seqsigwit_mode)?;
    Ok(true)
}

/// Largest create count `<= n` whose stem ends every body header still covers.
///
/// Stem ends grow with the count, so the covered counts are a prefix.
fn body_headers_cover(
    stems: &ClassASkewStems<'_>,
    n: u64,
    prune_seqsigwit_mode: bool,
) -> Result<u64, StoreError> {
    let fits = |count: u64| -> Result<bool, StoreError> {
        let (tx_end, sp_end, in_end) = class_a_skew_stem_ends(stems, count, prune_seqsigwit_mode)?;
        Ok(tx_end <= stems.body.body_logical_len()
            && sp_end <= stems.spent.body_logical_len()
            && (prune_seqsigwit_mode || in_end <= stems.seqsigwit.body_logical_len()))
    };
    if fits(n)? {
        return Ok(n);
    }
    // Invariant: fits(lo) and !fits(hi). Count 0 is the file headers.
    let (mut lo, mut hi) = (0u64, n);
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if fits(mid)? {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn class_a_loc_pairs(
    n: usize,
    n_outs: &[u32],
    p_out: &crate::var_table::PreparedAppend,
    p_sp: &crate::var_table::PreparedAppend,
) -> (
    Vec<crate::create_loc::CreateLocAppend>,
    Vec<crate::create_loc::CreateLocPair>,
) {
    let tx_lens = p_out.aligned_lens();
    let mut recs = Vec::with_capacity(n);
    let mut loc = Vec::with_capacity(n);
    for i in 0..n {
        let spent_len = spent_record_len(n_outs[i]);
        recs.push(crate::create_loc::CreateLocAppend {
            txout_start: p_out.starts[i],
            txout_len: tx_lens[i],
            spent_start: p_sp.starts[i],
            n_out: n_outs[i],
        });
        loc.push(crate::create_loc::CreateLocPair {
            txout: (p_out.starts[i], tx_lens[i]),
            spent: (p_sp.starts[i], spent_len),
            n_out: n_outs[i],
        });
    }
    (recs, loc)
}

/// `spent.body` bytes each create reserves: one 8-byte slot per output.
fn spent_lens(n_outs: &[u32]) -> Vec<u64> {
    n_outs
        .iter()
        .map(|&n| u64::from(n) * OutputRecord::SPENT_SLOT_LEN as u64)
        .collect()
}

#[cfg(debug_assertions)]
thread_local! {
    static AFTER_BODIES: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
        const { std::cell::RefCell::new(None) };
}

/// One-shot hook run inside the next Class A append on this thread, after
/// its bodies and locators publish and before `txid.body`, `txstat`, and
/// `input`. Tests run a spend commit or scripthash mark in that window.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_after_bodies_for_test(hook: Option<Box<dyn FnOnce()>>) {
    AFTER_BODIES.with(|c| *c.borrow_mut() = hook);
}

fn run_after_bodies_hook() {
    #[cfg(debug_assertions)]
    if let Some(hook) = AFTER_BODIES.with(|c| c.borrow_mut().take()) {
        hook();
    }
}

fn publish_prepared_body(
    table: &VarTable,
    prep: crate::var_table::PreparedAppend,
    durable: bool,
) -> Result<(Vec<Fk>, bool), StoreError> {
    let landed = table.note_body_end(prep.end, durable);
    let fks = table.finish_prepared(prep)?;
    Ok((fks, landed))
}

fn expect_same_fks(left: &[Fk], right: &[Fk]) -> Result<(), StoreError> {
    if left != right {
        return Err(StoreError::Corrupt(
            "Class A append fk mismatch across stems",
        ));
    }
    Ok(())
}

fn publish_body_pair(
    left: &VarTable,
    left_prep: crate::var_table::PreparedAppend,
    right: &VarTable,
    right_prep: crate::var_table::PreparedAppend,
    durable: bool,
) -> Result<(Vec<Fk>, bool), StoreError> {
    let (fks, left_ok) = publish_prepared_body(left, left_prep, durable)?;
    let (other, right_ok) = publish_prepared_body(right, right_prep, durable && left_ok)?;
    expect_same_fks(&fks, &other)?;
    Ok((fks, left_ok && right_ok))
}

#[cfg(debug_assertions)]
thread_local! {
    static SPEND_META_BATCHES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    static SPEND_META_PEEK_GROUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Zero the debug counters [`spend_meta_read_counts`] returns.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn reset_spend_meta_read_counts() {
    SPEND_META_BATCHES.with(|n| n.set(0));
    SPEND_META_PEEK_GROUPS.with(|n| n.set(0));
}

/// `(directory-or-body batch calls, body peek groups)` since the last reset.
///
/// A group is one [`crate::idx_body_pipeline::plan_spend_slot_peeks`] result.
/// Directory hits add a batch and zero groups.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn spend_meta_read_counts() -> (u64, u64) {
    (
        SPEND_META_BATCHES.with(|n| n.get()),
        SPEND_META_PEEK_GROUPS.with(|n| n.get()),
    )
}

fn note_spend_meta_read(groups: usize) {
    #[cfg(debug_assertions)]
    {
        SPEND_META_BATCHES.with(|n| n.set(n.get().saturating_add(1)));
        SPEND_META_PEEK_GROUPS.with(|n| n.set(n.get().saturating_add(groups as u64)));
    }
    #[cfg(not(debug_assertions))]
    {
        let _ = groups;
    }
}

/// `spent.body` as the spend delta folds into it. Every folded slot is below
/// the published end.
/// `spent.body` as the spend delta's fold target. After open, `table` is
/// the table whose Class A latch the sync runs under: the barrier syncs the
/// same fd. Open has no table yet, and no barrier runs before it returns.
struct SpentFold<'a> {
    spent: &'a VarTable,
    table: Option<&'a TxTable>,
}

impl<'a> SpentFold<'a> {
    fn at_open(spent: &'a VarTable) -> Self {
        Self { spent, table: None }
    }

    fn of(table: &'a TxTable) -> Self {
        Self {
            spent: &table.spent,
            table: Some(table),
        }
    }
}

impl crate::spend_delta::SpentBody for SpentFold<'_> {
    fn published_len(&self) -> u64 {
        self.spent.body_published_len()
    }

    fn read(&self, abs: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        let end = self.spent.body_published_len();
        self.spent
            .read_prefix_at_published(end, abs, buf.len() as u64, buf)?;
        Ok(())
    }

    fn write(&mut self, abs: u64, bytes: &[u8]) -> Result<(), StoreError> {
        self.spent.write_body_pwrite(abs, bytes)
    }

    fn sync(&mut self) -> Result<(), StoreError> {
        match self.table {
            Some(table) => table.sync_folded_spent(),
            None => self.spent.sync_data_only(),
        }
    }
}

/// Open the spend delta.
///
/// A `spent.body` written up to its published end was filled by an older
/// schema's Class A: a schema-30 body is sparse, and before its first
/// commit no fold has extended it. Such a body, and a schema-29 page log if
/// there is one, become the delta's first generation. `Store` removes the 29
/// files only after `meta` 30 is durable. A crash before that adopts again at
/// the next open, which skips a delta that already published. A populated
/// schema-30 body with no `spent.delta/` refuses: its spends were only there. A sparse body can reach its published
/// end on disk, so the length cannot tell the two apart; `opened_schema` (the
/// `meta` schema before this open) can. `Store` moves `meta` to 30 only after
/// this open returns, so a crash before the adoption publishes still opens
/// as pre-30 and adopts again. A table opened alone (`None`, the suite's bare
/// `TxTable` opens) keeps the length rule.
fn open_spend_delta(
    dir: &Path,
    spent: &VarTable,
    opened_schema: Option<u16>,
) -> Result<crate::spend_delta::SpendPageLog, StoreError> {
    let body_end = spent.body_published_len();
    let path = spent.body_file_path();
    let physical = std::fs::metadata(path)
        .map_err(|e| StoreError::io(path, e))?
        .len();
    let populated = body_end > crate::file::FILE_HEADER_LEN as u64;
    // 30 and 31 both keep a sparse `spent.body`. Only an older meta filled it.
    let spend_delta_meta = opened_schema.is_some_and(|v| v >= 30);
    let filled_by_class_a = physical >= body_end && !spend_delta_meta;
    if populated && !filled_by_class_a && !dir.join(crate::spend_delta::SPEND_DELTA_DIR).exists() {
        return Err(StoreError::Corrupt(
            "spent.delta is missing beside a populated spent.body; wipe datadir and redo IBD",
        ));
    }
    let log = crate::spend_delta::SpendPageLog::load_with(
        dir,
        &mut SpentFold::at_open(spent),
        crate::spend_delta::Policy::DEFAULT,
    )?;
    let v29 = crate::spend_pages_v29::read(dir, body_end)?;
    if populated && filled_by_class_a {
        let (images, tip) = v29.map_or((Vec::new(), 0), |v| (v.images, v.tip));
        log.adopt_v29(images, tip, &SpentFold::at_open(spent))?;
    }
    Ok(log)
}

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_opts(dir, HeadOpenOpts::MAINNET)
    }

    pub fn create_tiny(dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_opts(dir, HeadOpenOpts::TINY)
    }

    pub fn create_with_opts(dir: &Path, opts: HeadOpenOpts) -> Result<Self, StoreError> {
        Self::create_with_head_layout_opts(
            dir,
            crate::address_head::default_layout(opts.scale),
            opts,
        )
    }

    /// Create with an explicit head geometry (tests / recovery).
    pub fn create_with_head_layout(dir: &Path, layout: HeadLayout) -> Result<Self, StoreError> {
        Self::create_with_head_layout_opts(dir, layout, HeadOpenOpts::TINY)
    }

    pub fn create_with_head_layout_opts(
        dir: &Path,
        layout: HeadLayout,
        opts: HeadOpenOpts,
    ) -> Result<Self, StoreError> {
        Self::create_with_head_layout_seqsigwit(dir, dir, layout, opts)
    }

    /// Create Class A stems; `seqsigwit` may live in a different directory.
    pub fn create_with_head_layout_seqsigwit(
        dir: &Path,
        seqsigwit_dir: &Path,
        layout: HeadLayout,
        opts: HeadOpenOpts,
    ) -> Result<Self, StoreError> {
        if seqsigwit_dir != dir {
            std::fs::create_dir_all(seqsigwit_dir).map_err(|e| StoreError::io(seqsigwit_dir, e))?;
        }
        let secret = crate::store_secret::StoreSecret::load_or_create(dir)?;
        let layout = HeadLayout::new(layout.bits)?;
        let (seal_bits, workers) = Self::resolve_open_opts(opts);
        Ok(Self {
            body: VarTable::create(dir, "txout", TableKind::TxOut)?,
            seqsigwit: VarTable::create(seqsigwit_dir, "seqsigwit", TableKind::SeqSigWit)?,
            spent: VarTable::create_sparse_body(dir, "spent", TableKind::Spent)?,
            create_loc: crate::create_loc::CreateLoc::create(dir)?,
            seqsigwit_loc: crate::delta_loc::DeltaLoc::create(seqsigwit_dir, "seqsigwit")?,
            head: SegmentedTxHead::create(dir, layout)?,
            txids: crate::txid_body::TxidBody::create(dir)?,
            txstat: crate::txstat::TxStat::create(seqsigwit_dir)?,
            input: crate::input::Input::create(seqsigwit_dir)?,
            secret,
            pending_head: pending_head::PendingHeadInserts::new(),
            #[cfg(debug_assertions)]
            body_decodes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(debug_assertions)]
            outputs_decoded: std::sync::atomic::AtomicU64::new(0),
            #[cfg(debug_assertions)]
            input_edge_spans: std::sync::atomic::AtomicU64::new(0),
            rebuild_seal_bits: seal_bits,
            rebuild_workers: workers,
            prune_seqsigwit_mode: std::sync::atomic::AtomicBool::new(false),
            datadir_hdd: std::sync::atomic::AtomicBool::new(false),
            spend_pages: crate::spend_delta::SpendPageLog::create(dir)?,
            class_a_sync: std::sync::Mutex::new(()),
            class_a_failed: std::sync::atomic::AtomicBool::new(false),
            class_a_on_fail: std::sync::OnceLock::new(),
            hot_dir: dir.to_path_buf(),
            synced_named: std::sync::atomic::AtomicU64::new(0),
            settled: std::sync::atomic::AtomicU64::new(0),
        })
    }

    #[doc(hidden)]
    pub fn resolve_open_opts(opts: HeadOpenOpts) -> (u32, usize) {
        let seal_bits = opts
            .rebuild_seal_bits
            .map(|b| b.clamp(6, 26))
            .unwrap_or_else(|| {
                parse_rebuild_seal_bits(
                    std::env::var("RBITCOIN_TX_HEAD_REBUILD_SEAL_BITS")
                        .ok()
                        .as_deref(),
                )
            });
        let workers = opts
            .rebuild_workers
            .map(|n| n.clamp(1, 256))
            .unwrap_or_else(|| {
                if let Some(n) = parse_rebuild_workers(
                    std::env::var("RBITCOIN_TX_HEAD_REBUILD_WORKERS")
                        .ok()
                        .as_deref(),
                ) {
                    n
                } else {
                    tx_head_rebuild_workers_for_free_ram(
                        crate::sorted_run::logical_cpus(),
                        crate::sorted_run::mem_available_bytes().unwrap_or(0),
                    )
                }
            });
        (seal_bits, workers)
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        Self::open_with_opts(dir, HeadOpenOpts::MAINNET)
    }

    pub fn open_tiny(dir: &Path) -> Result<Self, StoreError> {
        Self::open_with_opts(dir, HeadOpenOpts::TINY)
    }

    pub fn open_with_opts(dir: &Path, opts: HeadOpenOpts) -> Result<Self, StoreError> {
        Self::open_seqsigwit(dir, dir, opts, None)
    }
}

/// What a [`crate::Store`] knows when it opens its [`TxTable`].
#[doc(hidden)]
#[derive(Clone, Copy, Debug)]
pub struct OpenedFromStore {
    /// The highest create a durable `header_txs` range names.
    pub committed: u64,
    /// The schema `meta` held before this open rewrote it.
    pub schema: u16,
}

impl TxTable {
    /// Open Class A stems; `seqsigwit` may live in a different directory.
    ///
    /// `store` is what the opening [`crate::Store`] knows ([`OpenedFromStore`]).
    /// Open keeps exactly the creates through its `committed`
    /// or [`CLASS_A_SYNCED_NAME`], whichever is higher. `None` (a table
    /// opened alone) keeps the prefix every file covers. See
    /// [`repair_class_a_count_skew`].
    #[doc(hidden)]
    pub fn open_seqsigwit(
        dir: &Path,
        seqsigwit_dir: &Path,
        opts: HeadOpenOpts,
        store: Option<OpenedFromStore>,
    ) -> Result<Self, StoreError> {
        crate::class_a_pages::prepare_and_restore(dir, seqsigwit_dir)?;
        crate::store::rename_legacy_inwit_files(dir)?;
        crate::store::rename_legacy_input_files(dir)?;
        crate::store::rename_legacy_inwit_files(seqsigwit_dir)?;
        crate::store::rename_legacy_input_files(seqsigwit_dir)?;
        // Sidecar lives on the hot store, including when the stem is cold.
        let prune_seqsigwit_mode = dir.join("seqsigwit.prune").is_file();
        let (seal_bits, workers) = Self::resolve_open_opts(opts);
        unlink_leftover_class_a_idx(dir)?;
        if seqsigwit_dir != dir {
            unlink_leftover_class_a_idx(seqsigwit_dir)?;
            std::fs::create_dir_all(seqsigwit_dir).map_err(|e| StoreError::io(seqsigwit_dir, e))?;
        }
        let had_txout = dir.join("txout.body").exists();
        let had_seqsigwit = seqsigwit_dir.join("seqsigwit.body").exists();
        let had_spent = dir.join("spent.body").exists();
        let synced_named = read_class_a_synced(dir)?;
        let named = store.map(|s| s.committed.max(synced_named));
        let at_least = named.unwrap_or(0);
        let create_loc = open_or_create_create_loc_above(dir, at_least)?;
        // A pruned witness locator is cut on purpose: no floor.
        let witness_floor = if prune_seqsigwit_mode { 0 } else { at_least };
        let seqsigwit_loc = open_or_create_seqsigwit_loc_above(seqsigwit_dir, witness_floor)?;
        let loc_count = create_loc.count();
        // The sidecar is prune mode for reads and later appends. The witness
        // stem still seals the tip until its locator no longer covers creates.
        let stem_retired = prune_seqsigwit_mode && seqsigwit_loc.count() < loc_count;
        let body = if had_txout {
            VarTable::open_body_only(dir, "txout", TableKind::TxOut, loc_count)?
        } else {
            VarTable::create_body_only(dir, "txout", TableKind::TxOut)?
        };
        if had_txout && loc_count > 0 && (!had_spent || (!had_seqsigwit && !stem_retired)) {
            return Err(StoreError::Corrupt(
                "schema 15 Class A missing seqsigwit/spent for existing txout creates; wipe + IBD \
                 (or --datadir-cold if seqsigwit is on a cold volume)",
            ));
        }
        let seqsigwit = if had_seqsigwit {
            let in_count = if stem_retired {
                seqsigwit_loc.count()
            } else {
                loc_count
            };
            VarTable::open_body_only(seqsigwit_dir, "seqsigwit", TableKind::SeqSigWit, in_count)?
        } else {
            VarTable::create_body_only(seqsigwit_dir, "seqsigwit", TableKind::SeqSigWit)?
        };
        let spent = if had_spent {
            VarTable::open_sparse_body(dir, "spent", TableKind::Spent, loc_count)?
        } else {
            VarTable::create_sparse_body(dir, "spent", TableKind::Spent)?
        };
        let txids = if dir.join("txid.body").exists() {
            crate::txid_body::TxidBody::open(dir)?
        } else {
            crate::txid_body::TxidBody::create(dir)?
        };
        if seqsigwit_dir != dir {
            for name in [
                "txstat.body",
                "txstat.ovf",
                "txstat.blk",
                "input.loc",
                "input.off",
                "input.body",
            ] {
                if dir.join(name).exists() && !seqsigwit_dir.join(name).exists() {
                    return Err(StoreError::Layout(format!(
                        "{name} is still in {}; move it next to seqsigwit under {}",
                        dir.display(),
                        seqsigwit_dir.display()
                    )));
                }
            }
        }
        let txstat = crate::txstat::TxStat::open(seqsigwit_dir)?;
        let input_existed = seqsigwit_dir.join("input.loc").exists();
        let input = if input_existed {
            crate::input::Input::open(seqsigwit_dir)?
        } else if seqsigwit_dir.join("input.body").exists()
            || seqsigwit_dir.join("input.off").exists()
        {
            return Err(StoreError::Corrupt("invariant: input stems partial"));
        } else {
            crate::input::Input::create(seqsigwit_dir)?
        };
        // A Class C fixture names creates with no archive behind it. Only an
        // archive has creates to refuse over.
        let archive = create_loc.count() > 0 || allocated_past_slack(&[&body, &spent, &seqsigwit]);
        let named = named.map(|n| if archive { n } else { 0 });
        repair_class_a_count_skew(
            ClassASkewStems {
                create_loc: &create_loc,
                seqsigwit_loc: &seqsigwit_loc,
                body: &body,
                spent: &spent,
                seqsigwit: &seqsigwit,
                txids: &txids,
                txstat: &txstat,
                input: &input,
            },
            stem_retired,
            input_existed,
            named,
        )?;
        let n_bodies = create_loc.count();
        if txstat.count() != n_bodies {
            rbitcoin_log::warn!(
                "store: txstat.body count={} loc={} — aligning to loc",
                txstat.count(),
                n_bodies
            );
            txstat.extend_or_truncate_to(n_bodies)?;
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
            SegmentedTxHead::create(dir, crate::address_head::default_layout(opts.scale))?
        } else {
            match SegmentedTxHead::open(dir) {
                Ok(h) => {
                    let covered = h.last_inserted_fk();
                    if covered > n_bodies && h.drop_segments_past(n_bodies)? {
                        // Inserts ran ahead of the barrier, and open cut
                        // their creates. The segments below the cut stay;
                        // the backfill below refills from the last kept fk.
                        rbitcoin_log::warn!(
                            "store: tx.head leads Class A covered={covered} n={n_bodies} \
                             — dropped segments past the cut, kept through {}",
                            h.last_inserted_fk()
                        );
                        h
                    } else if covered > n_bodies {
                        // No segment ends inside the cut: rebuild it all.
                        rbitcoin_log::warn!(
                            "store: tx.head leads Class A covered={covered} n={n_bodies} \
                             — wipe + rebuild"
                        );
                        drop(h);
                        crate::segmented_head::wipe_segmented_head_files(dir);
                        need_rebuild = n_bodies > 0;
                        SegmentedTxHead::create(
                            dir,
                            crate::address_head::default_layout(opts.scale),
                        )?
                    } else {
                        if n_bodies > 0 && h.occupied() == 0 {
                            need_rebuild = true;
                        }
                        h
                    }
                }
                Err(e) => {
                    if crate::segmented_head::is_index_open_refuse(&e) {
                        return Err(e);
                    }
                    if n_bodies > 0 {
                        rbitcoin_log::warn!(
                            "store: segmented tx.head unreadable ({e}) with {n_bodies} Class A \
                             bodies — recreate + rebuild"
                        );
                        crate::segmented_head::wipe_segmented_head_files(dir);
                        need_rebuild = true;
                        SegmentedTxHead::create(
                            dir,
                            crate::address_head::default_layout(opts.scale),
                        )?
                    } else {
                        return Err(e);
                    }
                }
            }
        };
        let secret = crate::store_secret::StoreSecret::load_existing(dir)?;
        let spend_pages = open_spend_delta(dir, &spent, store.map(|s| s.schema))?;
        let t = Self {
            body,
            seqsigwit,
            spent,
            create_loc,
            seqsigwit_loc,
            head,
            txids,
            txstat,
            input,
            secret,
            pending_head: pending_head::PendingHeadInserts::new(),
            #[cfg(debug_assertions)]
            body_decodes: std::sync::atomic::AtomicU64::new(0),
            #[cfg(debug_assertions)]
            outputs_decoded: std::sync::atomic::AtomicU64::new(0),
            #[cfg(debug_assertions)]
            input_edge_spans: std::sync::atomic::AtomicU64::new(0),
            rebuild_seal_bits: seal_bits,
            rebuild_workers: workers,
            prune_seqsigwit_mode: std::sync::atomic::AtomicBool::new(prune_seqsigwit_mode),
            datadir_hdd: std::sync::atomic::AtomicBool::new(false),
            spend_pages,
            class_a_sync: std::sync::Mutex::new(()),
            class_a_failed: std::sync::atomic::AtomicBool::new(false),
            class_a_on_fail: std::sync::OnceLock::new(),
            hot_dir: dir.to_path_buf(),
            synced_named: std::sync::atomic::AtomicU64::new(synced_named),
            settled: std::sync::atomic::AtomicU64::new(n_bodies),
        };
        match input_open_tail(t.input.count(), n_bodies, t.seqsigwit.count()) {
            InputOpenTail::Backfill { from } => {
                // Legacy inline prevouts (and a missing file, which starts at
                // fk 1) rebuild edges from seqsigwit. A new-layout tail has no
                // inline prevout: stamping n_in = 0 would seal those creates
                // as having no parents.
                let legacy = from == 1 || t.seqsigwit_has_inline_prevout(Fk(from))?;
                if !legacy {
                    return Err(StoreError::Corrupt("invariant: input tail missing"));
                }
                t.backfill_inputs_from_seqsigwit(from)?;
            }
            InputOpenTail::Unstamped { .. } => {
                return Err(StoreError::Corrupt("invariant: input tail missing"));
            }
            InputOpenTail::Ahead => {
                return Err(StoreError::Corrupt(
                    "invariant: input.loc ahead of create.loc",
                ));
            }
            InputOpenTail::Ready => {}
        }
        if n_bodies > 0 {
            let _ = t.input.n_in(Fk(1))?;
        }
        if need_rebuild {
            let bits = t.head_bits();
            let slots = t.head_slots();
            rbitcoin_log::info!(
                "store: tx.head rebuild begin n={n_bodies} bits={bits} slots={slots} \
                 seal_bits={} workers={} free_GiB={} (segmented)",
                t.rebuild_seal_bits(),
                t.rebuild_workers(),
                crate::free_gib_label(),
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
            // A missing or generation-mismatched snapshot lost the whole open
            // tail. Rebuild that tail from `txid.body` before any probe.
            // A matching snapshot only backfills creates appended after it.
            // Seal the in-flight segment before the tail rebuild can roll.
            t.seal_unsealed_nontail_from_body()?;
            let n = t.count();
            let covered = t.head.last_inserted_fk();
            if let Some(first) = t.head.open_tail_rebuild_from()? {
                rbitcoin_log::info!(
                    "store: tx.head open tail snapshot missing first_fk={first} \
                     covered={covered} n={n} — rebuild tail"
                );
                t.backfill_head_from(first)?;
                t.head.flush()?;
            } else if covered < n {
                rbitcoin_log::info!(
                    "store: tx.head lags Class A covered={covered} n={n} — backfill tail"
                );
                t.backfill_head_from(covered.saturating_add(1))?;
                t.head.flush()?;
            }
            t.tag_untagged_open_heads()?;
        }
        Ok(t)
    }

    /// A meta v1 head predates slot fingerprints: every row on a chain is an
    /// identity candidate. Mix the open segment's txids from `txid.body`,
    /// write each row's fingerprint into its slot, then record the segment as
    /// tagged. Runs once per upgraded datadir.
    fn tag_untagged_open_heads(&self) -> Result<(), StoreError> {
        let n_body = self.count();
        for (file_id, first_fk, count) in self.head.untagged_open_ranges() {
            let last = first_fk.saturating_add(count).saturating_sub(1);
            if count == 0 || first_fk == 0 || last > n_body {
                continue;
            }
            let t0 = std::time::Instant::now();
            let workers = self.rebuild_workers().max(1) as u64;
            let span = count.div_ceil(workers).max(1);
            std::thread::scope(|sc| {
                let handles: Vec<_> = (first_fk..=last)
                    .step_by(span as usize)
                    .map(|cur| {
                        let end = (cur + span - 1).min(last);
                        sc.spawn(move || self.tag_head_range(file_id, cur, end))
                    })
                    .collect();
                for h in handles {
                    h.join()
                        .map_err(|_| StoreError::Corrupt("tx.head fingerprint fill panicked"))??;
                }
                Ok::<(), StoreError>(())
            })?;
            // Slots durable before the meta says they are tagged.
            self.head.flush()?;
            self.head.mark_tagged(file_id)?;
            rbitcoin_log::info!(
                "store: tx.head fingerprints written file_id={file_id} first_fk={first_fk} \
                 count={count} workers={workers} ms={}",
                t0.elapsed().as_millis()
            );
        }
        Ok(())
    }

    fn tag_head_range(&self, file_id: u32, first: u64, last: u64) -> Result<(), StoreError> {
        const CHUNK: u64 = 65_536;
        let mut cur = first;
        while cur <= last {
            let end = (cur + CHUNK - 1).min(last);
            let keys: Vec<([u8; 32], Fk)> = self
                .body_txid_range(cur, end)?
                .iter()
                .zip(cur..)
                .map(|(txid, fk)| (self.secret.mix_txid(txid), Fk(fk)))
                .collect();
            self.head.fill_fingerprints(file_id, &keys)?;
            cur = end + 1;
        }
        Ok(())
    }

    pub fn prune_seqsigwit_mode(&self) -> bool {
        self.prune_seqsigwit_mode
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn set_prune_seqsigwit_mode(&self, on: bool) {
        self.prune_seqsigwit_mode
            .store(on, std::sync::atomic::Ordering::Release);
    }

    pub fn datadir_hdd(&self) -> bool {
        self.datadir_hdd.load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn set_datadir_hdd(&self, on: bool) {
        self.datadir_hdd
            .store(on, std::sync::atomic::Ordering::Release);
    }

    pub fn clear_durable_seqsigwit(&self) -> Result<(), StoreError> {
        self.seqsigwit_loc.truncate_to_count(0)?;
        self.seqsigwit
            .truncate_body_to(0, crate::file::FILE_HEADER_LEN as u64)
    }

    /// The witness stem still holds rows or an allocation past create slack.
    pub fn seqsigwit_stem_needs_release(&self) -> bool {
        let header = crate::file::FILE_HEADER_LEN as u64;
        if self.seqsigwit_loc.count() > 0 || self.seqsigwit.body_logical_len() > header {
            return true;
        }
        self.seqsigwit
            .body
            .file_cap
            .load(std::sync::atomic::Ordering::Acquire)
            > header.saturating_add(crate::file::CREATE_FILE_SLACK)
    }

    /// Inputs from `seqsigwit.body` even when prune mode makes [`Self::get_full`]
    /// return [`StoreError::NotFound`]. The locator must still name `fk`.
    pub fn seqsigwit_inputs_from_stem(&self, fk: Fk) -> Result<Vec<InputRecord>, StoreError> {
        let tx = self.get(fk)?;
        let ir = self
            .seqsigwit_loc
            .range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)?;
        let seqsigwit = self
            .seqsigwit
            .with_bytes_at(ir.0, ir.1, |b| Ok(b.to_vec()))?;
        let mut ins = if tx.input_count == 0 {
            decode_seqsigwit_secret_to_end(&seqsigwit, Some(&self.secret))?
        } else {
            decode_seqsigwit_secret(&seqsigwit, tx.input_count, Some(&self.secret))?
        };
        self.stamp_seqsigwit_prevouts(fk, &mut ins)?;
        Ok(ins)
    }

    /// Drop the witness stem's rows and its allocation back to the file header.
    pub fn cut_seqsigwit_stem_to_header(&self) -> Result<(), StoreError> {
        let header = crate::file::FILE_HEADER_LEN as u64;
        // Loc header at 0 is durable before off and ovf shrink. The other
        // order leaves a long locator and a header-only checkpoint.
        self.seqsigwit_loc.truncate_to_zero_header_first()?;
        self.seqsigwit_loc.loc.release_to_len(header)?;
        self.seqsigwit_loc.off.release_to_len(header)?;
        self.seqsigwit_loc.ovf.release_to_len(header)?;
        self.seqsigwit.truncate_body_to(0, header)?;
        self.seqsigwit.body.release_to_len(header)?;
        // The short length is the durable end. A later open must not see
        // the pre-cut allocation and treat it as witness payload.
        self.seqsigwit_loc.loc.flush()?;
        self.seqsigwit_loc.off.flush()?;
        self.seqsigwit_loc.ovf.flush()?;
        self.seqsigwit.body.flush()?;
        Ok(())
    }

    /// Seal unsealed non-tails from Class A (crash/restart). Keys are not retained.
    #[doc(hidden)]
    pub fn seal_unsealed_nontail_from_body(&self) -> Result<(), StoreError> {
        let n_body = self.count();
        for (file_id, first_fk, count) in self.head.unsealed_nontail_ranges() {
            if count == 0 {
                continue;
            }
            let last_fk = first_fk.saturating_add(count).saturating_sub(1);
            if first_fk == 0 || first_fk > n_body || last_fk > n_body {
                rbitcoin_log::warn!(
                    "store: skip unsealed fuse rebuild file_id={file_id} first={first_fk} \
                     count={count} body={n_body}; head may lead truncated Class A"
                );
                continue;
            }
            let pairs = self.fuse_pairs_for_range(first_fk, count)?;
            self.head.seal_file_sync(file_id, pairs)?;
            rbitcoin_log::info!(
                "store: tx.head unsealed fuse keys sealed file_id={file_id} \
                 first_fk={first_fk} count={count}"
            );
        }
        Ok(())
    }

    /// `(fuse_key, rel)` pairs for a create fk range (1-based `first_fk`, `count`).
    #[doc(hidden)]
    pub fn fuse_pairs_for_range(
        &self,
        first_fk: u64,
        count: u64,
    ) -> Result<Vec<(u64, u32)>, StoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        const CHUNK: u64 = 65_536;
        let last = first_fk.saturating_add(count).saturating_sub(1);
        let mut pairs = Vec::with_capacity(count as usize);
        let mut rel = 1u32;
        let mut cur = first_fk;
        while cur <= last {
            let end = (cur + CHUNK - 1).min(last);
            let txids = self.body_txid_range(cur, end)?;
            for txid in txids {
                pairs.push((
                    crate::fuse8_filter::fuse_key_from_mixed(&self.secret.mix_txid(&txid)),
                    rel,
                ));
                rel = rel.saturating_add(1);
            }
            cur = end + 1;
        }
        if pairs.len() as u64 != count {
            return Err(StoreError::Corrupt(
                "tx.head unsealed body range count mismatch",
            ));
        }
        Ok(pairs)
    }

    /// Sidecar collect: libc pread of `[first_fk, first_fk+count)` (already
    /// published on `txid.body` before head insert).
    #[doc(hidden)]
    pub fn fuse_pairs_from_txid_pread(
        fd: crate::io_handle::IoHandle,
        path: &Path,
        secret: &crate::store_secret::StoreSecret,
        first_fk: u64,
        count: u64,
    ) -> Result<Vec<(u64, u32)>, StoreError> {
        if count == 0 {
            return Ok(Vec::new());
        }
        const CHUNK: u64 = 65_536;
        let last = first_fk.saturating_add(count).saturating_sub(1);
        let mut pairs = Vec::with_capacity(count as usize);
        let mut rel = 1u32;
        let mut cur = first_fk;
        while cur <= last {
            let end = (cur + CHUNK - 1).min(last);
            let n = (end - cur + 1) as usize;
            let off = crate::txid_body::TxidBody::entry_offset(cur)?;
            let mut blob = vec![0u8; n * 32];
            let rc = crate::bulk_io::pread_single(fd, off, &mut blob);
            if rc < 0 {
                return Err(StoreError::io(path, std::io::Error::from_raw_os_error(-rc)));
            }
            if (rc as usize) != blob.len() {
                return Err(StoreError::Corrupt("tx.head seal collect txid.body short"));
            }
            for txid in blob.as_chunks::<32>().0 {
                pairs.push((
                    crate::fuse8_filter::fuse_key_from_mixed(&secret.mix_txid(txid)),
                    rel,
                ));
                rel = rel.saturating_add(1);
            }
            cur = end + 1;
        }
        if pairs.len() as u64 != count {
            return Err(StoreError::Corrupt(
                "tx.head unsealed body range count mismatch",
            ));
        }
        Ok(pairs)
    }

    pub fn count(&self) -> u64 {
        self.body.count()
    }

    /// Creates an append has finished: every Class A row, not just the
    /// bodies [`Self::count`] covers. A record that names creates samples
    /// this, so an append still in flight is never named.
    pub fn settled_count(&self) -> u64 {
        self.settled.load(std::sync::atomic::Ordering::Acquire)
    }

    fn settle(&self, base: u64, n: usize) {
        self.settled.store(
            base.saturating_add(n as u64),
            std::sync::atomic::Ordering::Release,
        );
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
        self.create_loc_range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .map(|p| p.txout)
            .ok_or(StoreError::NotFound)
    }

    pub fn create_loc_range_batch(
        &self,
        fks: &[Fk],
    ) -> Result<Vec<Option<crate::create_loc::CreateLocPair>>, StoreError> {
        self.create_loc.range_batch(fks)
    }

    /// Durable `create.loc` row count (not `tx.body` HWM).
    pub fn create_loc_count(&self) -> u64 {
        self.create_loc.count()
    }

    /// Truncate `create.loc` to `n` rows without touching Class A bodies.
    pub fn create_loc_truncate_to_count(&self, n: u64) -> Result<(), StoreError> {
        self.create_loc.truncate_to_count(n)
    }

    pub fn fill_txout_job_ranges(&self, jobs: &mut [crate::IdxBodyJob]) -> Result<(), StoreError> {
        let mut need = Vec::new();
        let mut slots = Vec::new();
        for (i, j) in jobs.iter().enumerate() {
            if j.id > 0 && (j.range.is_none() || j.n_out == 0) {
                need.push(Fk(j.id));
                slots.push(i);
            }
        }
        if need.is_empty() {
            return Ok(());
        }
        let pairs = self.create_loc_range_batch(&need)?;
        for (slot, p) in slots.into_iter().zip(pairs) {
            if let Some(p) = p {
                if jobs[slot].range.is_none() {
                    jobs[slot].range = Some(p.txout);
                }
                if jobs[slot].n_out == 0 {
                    jobs[slot].n_out = p.n_out;
                }
            }
        }
        Ok(())
    }

    pub fn fill_seqsigwit_job_ranges(
        &self,
        jobs: &mut [crate::IdxBodyJob],
    ) -> Result<(), StoreError> {
        if self.prune_seqsigwit_mode() {
            return Ok(());
        }
        let mut need = Vec::new();
        let mut slots = Vec::new();
        for (i, j) in jobs.iter().enumerate() {
            if j.range.is_none() && j.id > 0 {
                need.push(Fk(j.id));
                slots.push(i);
            }
        }
        if need.is_empty() {
            return Ok(());
        }
        let pairs = self.seqsigwit_loc.range_batch(&need)?;
        for (slot, p) in slots.into_iter().zip(pairs) {
            jobs[slot].range = p;
        }
        Ok(())
    }

    /// One sequential `tx.body` pread of `[offset, offset+len)`.
    pub fn with_body_span<R>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnOnce(&[u8]) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        let mut buf = Vec::new();
        self.with_body_span_into(offset, len, &mut buf, f)
    }

    pub fn with_body_span_into<R>(
        &self,
        offset: u64,
        len: u64,
        buf: &mut Vec<u8>,
        f: impl FnOnce(&[u8]) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        self.body.with_bytes_at_into(offset, len, buf, f)
    }

    /// Contiguous Class A body `(offset, len)` for create_fks `first..=last`.
    pub fn body_ranges(&self, first: u64, last: u64) -> Result<Vec<(u64, u64)>, StoreError> {
        if first == 0 || last < first {
            return Ok(Vec::new());
        }
        let fks: Vec<Fk> = (first..=last).map(Fk).collect();
        let pairs = self.create_loc.range_batch(&fks)?;
        let mut out = Vec::with_capacity(pairs.len());
        for p in pairs {
            out.push(p.ok_or(StoreError::NotFound)?.txout);
        }
        Ok(out)
    }

    /// P2TR outs from a packed body slice (stack XOR; no `OutputRecord` heap).
    pub fn packed_p2tr_from_raw(
        &self,
        raw: &[u8],
        n_out: u32,
    ) -> Result<Vec<(u32, [u8; 32], u64)>, StoreError> {
        scan_packed_p2tr_outs(raw, n_out, Some(&self.secret))
    }

    /// Meta + input prevouts only (no script/witness allocation, no outputs).
    ///
    /// Stamped rows read `n_in` and the parent edge from `input`. Leftover
    /// unstamped rows still take the inline prevout out of legacy `seqsigwit`.
    pub fn get_meta_and_prevouts(&self, fk: Fk) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
        // Stamped rows read `input`. Prune drops script and witness, not edges.
        // An unstamped row still needs the witness stem, which prune has cut.
        let mut tx = if let Some(n_in) = self.input.n_in(fk)? {
            TxRecord {
                txid: [0u8; 32],
                version: 0,
                locktime: 0,
                input_count: n_in,
                output_count: 0,
            }
        } else if self.prune_seqsigwit_mode() {
            return Err(StoreError::NotFound);
        } else {
            self.get(fk)?
        };
        let prevs = if let Some(edges) = self.input.edges(fk)? {
            prevouts_from_edges(&edges)
        } else if self.prune_seqsigwit_mode() {
            return Err(StoreError::NotFound);
        } else {
            let ir = self
                .seqsigwit_loc
                .range_batch(&[fk])?
                .into_iter()
                .next()
                .flatten()
                .ok_or(StoreError::NotFound)?;
            let seqsigwit = self
                .seqsigwit
                .with_bytes_at(ir.0, ir.1, |b| Ok(b.to_vec()))?;
            scan_seqsigwit_prevouts(&seqsigwit, tx.input_count)?
        };
        tx.txid = self.txids.get(fk)?;
        Ok((tx, prevs))
    }

    pub fn overlay_stamped_n_in(&self, fk: Fk, tx: &mut TxRecord) -> Result<(), StoreError> {
        let Some(n_in) = self.input.n_in(fk)? else {
            return Ok(());
        };
        if tx.input_count != 0 && tx.input_count != n_in {
            return Err(StoreError::Corrupt("input n_in mismatch txout"));
        }
        tx.input_count = n_in;
        Ok(())
    }

    pub fn reserve_append(&self, body_bytes: u64, n_records: u64) -> Result<(), StoreError> {
        self.body.reserve_append(body_bytes, n_records)
    }

    /// Meta by fk: the same packed decode as [`Self::get_meta_and_outputs`]
    /// with the outs dropped (`txout` never carries ins).
    pub fn get(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        self.get_meta_and_outputs(fk).map(|(tx, _)| tx)
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
    pub fn probe_body_match_fk(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        use std::time::Instant;
        let mixed = self.secret.mix_txid(txid);
        let t_probe = Instant::now();
        let cands = self.head.probe_candidates(&mixed)?;
        crate::head_resolve_stats::add_probe(t_probe.elapsed().as_nanos() as u64);
        crate::head_resolve_stats::add_keys(1);
        crate::head_resolve_stats::add_cands(cands.len() as u64);
        for (i, fk) in cands.into_iter().enumerate() {
            if self.body_txid(fk)? == *txid {
                let mut hit = crate::head_resolve_stats::HitRanks::default();
                hit.note((i as u64).saturating_add(1));
                crate::head_resolve_stats::add_hit_ranks(&hit);
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
    /// **per-key depth-first** sidefile identity (io_uring when available) → loc
    /// range on hit. **No** cross-key depth-round batching. Prep denserels-loads
    /// via known `body_range` (skip loc).
    ///
    /// BIP30: deepest matching create wins (probe order deepest-first).
    /// Timers: [`crate::head_resolve_stats`] probe / idx / body.
    pub fn get_fk_by_txid_batch(&self, txids: &[[u8; 32]]) -> Result<Vec<TxidFkRange>, StoreError> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        crate::head_resolve_denserels::resolve_fk_and_range_batch(self, txids)
    }

    /// Sparse outs by known `txout` body ranges (prep pin after plan stamp).
    ///
    /// Each job is `(create_fk, body_range, known_txid, n_out, need_vouts)`.
    /// - **Skips loc** (range known).
    /// - **`known_txid`**: RAM identity (plan reverse map / residency); not sidefile.
    /// - **`need_vouts`**: sorted unique; empty = all outs. Only those scripts are
    ///   allocated (N2.1). First-wave Outs peek is the remainder of the starting
    ///   OS page unless `(max_vout+1)*40` (empty need: the loc span) is likely to
    ///   spill onto the next page — then the first wave is the full loc span.
    ///
    /// Returns `(rows, body_ns, decode_ns, io)` where each row is
    /// `Some((tx, live (vout,out), sparse denserels (vout,rel)))` (N2.0 timers).
    pub fn get_outs_by_range_batch(
        &self,
        items: &[OutsByRangeJob],
    ) -> Result<OutsByRangeOut, StoreError> {
        use crate::idx_body_pipeline::{run_idx_body_pipeline_backend, BodyMode, IdxBodyJob};
        use std::time::Instant;
        if items.is_empty() {
            return Ok((Vec::new(), 0, 0, Default::default()));
        }
        let mut jobs: Vec<IdxBodyJob> = items
            .iter()
            .map(|(fk, range, _txid, n_out, need)| {
                let mut job = IdxBodyJob::new(fk.get().unwrap_or(0), Some(*range));
                job.need_vouts = need.clone();
                job.n_out = *n_out;
                job
            })
            .collect();
        let t_body = Instant::now();
        let io = run_idx_body_pipeline_backend(
            &self.body,
            &mut jobs,
            BodyMode::Outs,
            crate::io_backend::read_io_backend(),
        )?;
        let body_ns = t_body.elapsed().as_nanos() as u64;
        let secret = self.store_secret();
        let t_dec = Instant::now();
        let mut out = Vec::with_capacity(jobs.len());
        for (job, (_fk, _range, known_txid, n_out, need)) in jobs.into_iter().zip(items.iter()) {
            if !job.ok || job.body.is_empty() {
                out.push(None);
                continue;
            }
            match decode_packed_tx_need_outs_with_spender_rels_secret(
                &job.body,
                *n_out,
                need,
                Some(secret),
            ) {
                Ok((mut tx, live, sparse)) => {
                    tx.txid = *known_txid;
                    // input_count stays 0. Full get overlays n_in from input.loc.
                    out.push(Some((tx, live, sparse)));
                }
                Err(StoreError::NotFound) | Err(StoreError::Corrupt(_)) => out.push(None),
                Err(e) => return Err(e),
            }
        }
        let decode_ns = t_dec.elapsed().as_nanos() as u64;
        Ok((out, body_ns, decode_ns, io))
    }

    /// Bulk `body_range` for many fks (confirm load / reconstruct).
    ///
    /// Thin wrapper over [`Self::create_loc_range_batch`] (txout half).
    pub fn body_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        Ok(self
            .create_loc
            .range_batch(fks)?
            .into_iter()
            .map(|p| p.map(|x| x.txout))
            .collect())
    }

    #[doc(hidden)]
    pub fn seqsigwit_has_inline_prevout(&self, fk: Fk) -> Result<bool, StoreError> {
        // `seqsigwit_range` refuses every row once prune mode is on. The
        // stem can still name this create. Backfill has to read that row.
        let Some(Some((off, len))) = self.seqsigwit_loc.range_batch(&[fk])?.into_iter().next()
        else {
            return Ok(false);
        };
        if len == 0 {
            return Ok(false);
        }
        let flags = self.seqsigwit.with_bytes_at(off, 1, |b| Ok(b[0]))?;
        Ok(flags & input_flags::PREV_ON_INPUTS == 0)
    }

    #[doc(hidden)]
    pub fn backfill_inputs_from_seqsigwit(&self, from: u64) -> Result<(), StoreError> {
        let n = self.create_loc.count();
        let mut id = from.max(1);
        rbitcoin_log::info!("store: input backfill from seqsigwit n={n} start={id}");
        let live = rbitcoin_log::progress::begin_at("input backfill", n, id - 1);
        let mut buf = Vec::new();
        while id <= n {
            let end = backfill_chunk_end(id, n);
            let fks: Vec<Fk> = (id..=end).map(Fk).collect();
            let ranges = self.seqsigwit_loc.range_batch(&fks)?;
            let edges = edges_from_seqsigwit_ranges(
                &mut |off, len, buf| self.seqsigwit.with_bytes_at_into(off, len, buf, |_| Ok(())),
                &ranges,
                INPUT_BACKFILL_SPAN,
                &mut buf,
            )?;
            self.input.append(&edges)?;
            live.set_done(end);
            if backfill_progress_due(end, id) {
                rbitcoin_log::info!("store: input backfill progress {end}/{n}");
            }
            let next_id = end + 1;
            if next_id <= id {
                return Err(StoreError::Corrupt(
                    "invariant: input backfill did not advance",
                ));
            }
            id = next_id;
        }
        rbitcoin_log::info!("store: input backfill complete n={n}");
        // The witness stem can be cut after this open returns. The edges
        // have to be durable first, or the next open has no way to rebuild them.
        self.flush_input()?;
        Ok(())
    }

    /// `seqsigwit.body` range for one create.
    pub fn seqsigwit_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        if self.prune_seqsigwit_mode() {
            return Err(StoreError::NotFound);
        }
        self.seqsigwit_loc
            .range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)
    }

    pub fn spent_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.create_loc_range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .map(|p| p.spent)
            .ok_or(StoreError::NotFound)
    }

    /// `spent.body` ranges (same fk order as [`Self::body_range_batch`]).
    pub fn spent_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        Ok(self
            .create_loc
            .range_batch(fks)?
            .into_iter()
            .map(|p| p.map(|x| x.spent))
            .collect())
    }

    /// Annotate spends at known absolute spender-meta offsets.
    ///
    /// One read of the current slot, then a page-log insert. A slot past the
    /// published body is cold and is not written.
    /// Decide sole / multi / promote from that slot.
    /// `spent.ovf` appends run inline on the decision.
    ///
    /// Returns edges past the published body. Multi-list cases are handled here.
    pub fn put_spend_batch_by_abs_meta(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        abs_edges: &[(u64, Fk, u32, Fk, u32)],
    ) -> Result<Vec<(Fk, u32, Fk, u32)>, StoreError> {
        if abs_edges.is_empty() {
            return Ok(Vec::new());
        }
        for &(_, _, _, sfk, _) in abs_edges {
            if sfk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }
        let known = self.known_spend_slots(abs_edges)?;
        let body_pub = self.spent.body_published_len();
        let mut cold = Vec::new();
        let mut live_edges = Vec::with_capacity(abs_edges.len());
        let mut live_known = Vec::with_capacity(abs_edges.len());
        for (edge, slot) in abs_edges.iter().copied().zip(known) {
            let (abs, cfk, vout, sfk, vin) = edge;
            if abs.saturating_add(OutputRecord::SPENT_SLOT_LEN as u64) > body_pub {
                cold.push((cfk, vout, sfk, vin));
                continue;
            }
            live_edges.push(edge);
            live_known.push(slot);
        }
        if !live_edges.is_empty() {
            cold.extend(self.put_spend_batch_by_abs_meta_known(
                spenders,
                &live_edges,
                &live_known,
            )?);
        }
        Ok(cold)
    }

    /// One directory-or-body read for the whole batch.
    ///
    /// A slot past the published body is `(NULL, 0, 0)` and is not read. A
    /// short or undecodable in-range slot is read again here so that error
    /// still fails the annotate.
    fn known_spend_slots(
        &self,
        abs_edges: &[(u64, Fk, u32, Fk, u32)],
    ) -> Result<Vec<(Fk, u8, u32)>, StoreError> {
        let offs: Vec<u64> = abs_edges.iter().map(|edge| edge.0).collect();
        let peeked = self.get_spender_meta_at_abs_batch(&offs)?;
        let body_pub = self.spent.body_published_len();
        let mut known = Vec::with_capacity(abs_edges.len());
        for (edge, slot) in abs_edges.iter().zip(peeked) {
            let past = edge.0.saturating_add(OutputRecord::SPENT_SLOT_LEN as u64) > body_pub;
            known.push(match slot {
                Some((field, flags, vin)) => (field, flags, vin),
                None if past => (Fk::NULL, 0, 0),
                None => self.body_spend_slot(edge.0)?,
            });
        }
        Ok(known)
    }

    /// Body slot at `abs`. The batch peek already skipped a directory hit.
    fn body_spend_slot(&self, abs: u64) -> Result<(Fk, u8, u32), StoreError> {
        let published = self.spent.body_published_len();
        let mut buf = [0u8; OutputRecord::SPENT_SLOT_LEN];
        self.spent.read_prefix_at_published(
            published,
            abs,
            OutputRecord::SPENT_SLOT_LEN as u64,
            &mut buf,
        )?;
        let (flags, field, vin) = decode_spent_slot(&buf)?;
        Ok((field, flags, vin))
    }

    /// Bulk 8-byte spender meta reads at absolute `spent.body` file offsets.
    ///
    /// Returns `(spender_field, flags)` — multi = `flags & MULTI_SPENDER`.
    /// Backend from [`spend_meta_backend`] / global `RBITCOIN_IO` /
    /// global `RBITCOIN_IO` (`uring` \| `pread`). Out-of-range / short → `None`.
    pub fn get_spender_meta_at_abs_batch(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<SpenderSlot>>, StoreError> {
        self.get_spender_meta_at_abs_batch_backend(abs_offs, spend_meta_backend())
            .map(|(slots, _)| slots)
    }

    /// Like [`Self::get_spender_meta_at_abs_batch`] with an explicit backend,
    /// plus what the `spent.body` peeks read.
    pub fn get_spender_meta_at_abs_batch_backend(
        &self,
        abs_offs: &[u64],
        backend: crate::io_backend::ReadIoBackend,
    ) -> Result<SpendMetaRead, StoreError> {
        use crate::io_backend::ReadIoBackend;
        if abs_offs.is_empty() {
            return Ok((Vec::new(), Default::default()));
        }
        match backend {
            ReadIoBackend::Uring => {
                match self.get_spender_meta_at_abs_batch_fd(abs_offs, ReadIoBackend::Uring) {
                    Ok(v) => Ok(v),
                    Err(e) => {
                        rbitcoin_log::debug!(
                            Store,
                            "structural meta uring failed ({e}); pread fallback"
                        );
                        self.get_spender_meta_at_abs_batch_fd(abs_offs, ReadIoBackend::Pread)
                    }
                }
            }
            ReadIoBackend::Pread => {
                self.get_spender_meta_at_abs_batch_fd(abs_offs, ReadIoBackend::Pread)
            }
        }
    }

    #[doc(hidden)]
    pub fn get_spender_meta_at_abs_batch_fd(
        &self,
        abs_offs: &[u64],
        backend: crate::io_backend::ReadIoBackend,
    ) -> Result<SpendMetaRead, StoreError> {
        const META_LEN: usize = OutputRecord::SPENT_SLOT_LEN;
        let body_fd = self.spent.body_read_fd();
        let body_pub = self.spent.body_published_len();
        let body_path = self.spent.body_file_path();

        let mut out: Vec<Option<SpenderSlot>> = vec![None; abs_offs.len()];
        let logged = self.spend_pages.slots_at_backend(abs_offs, backend)?;
        let mut submitted: Vec<usize> = Vec::with_capacity(abs_offs.len());
        for (i, &off) in abs_offs.iter().enumerate() {
            let end = off.saturating_add(META_LEN as u64);
            if end > body_pub {
                continue;
            }
            if let Some(raw) = logged[i] {
                let (flags, field, vin) = decode_spent_slot(&raw)?;
                out[i] = Some((field, flags, vin));
                continue;
            }
            submitted.push(i);
        }
        if submitted.is_empty() {
            note_spend_meta_read(0);
            return Ok((out, Default::default()));
        }

        let live: Vec<u64> = submitted.iter().map(|&i| abs_offs[i]).collect();
        let plan = crate::idx_body_pipeline::plan_spend_slot_peeks(&live);
        note_spend_meta_read(plan.len());
        let depth = crate::uring_session::DEFAULT_ENTRIES as usize;
        let mut io = crate::idx_body_pipeline::PeekIo::default();
        for chunk in plan.chunks(depth.max(1)) {
            io.add(read_spend_peek_chunk(
                chunk, &live, &submitted, &mut out, body_fd, body_path, backend,
            )?);
        }
        Ok((out, io))
    }

    /// Directory spend annotate using structural-known meta.
    ///
    /// `known[i]` is `(field, flags, vin)` at `abs_edges[i].0` from structural spentness.
    /// A published slot is inserted into the spend directory. This does not
    /// read or write a `spent.body` page. Returns cold edges (OOB). Production
    /// callers must treat non-empty as a hard error.
    pub fn put_spend_batch_by_abs_meta_known(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        abs_edges: &[(u64, Fk, u32, Fk, u32)],
        known: &[(Fk, u8, u32)],
    ) -> Result<Vec<(Fk, u32, Fk, u32)>, StoreError> {
        crate::spend_annotate::put_spend_batch_by_abs_meta_known(self, spenders, abs_edges, known)
    }

    /// Read multi + spender_field for create tx output (packed Class A body).
    pub fn get_output_spender_meta(
        &self,
        create_tx_fk: Fk,
        vout: u32,
    ) -> Result<(bool, Fk, u32), StoreError> {
        let (off, len) = self.spent_range(create_tx_fk)?;
        self.get_output_spender_meta_at(off, len, vout)
    }

    /// Like [`Self::get_output_spender_meta`] but uses a cache-held body range (no idx).
    pub fn get_output_spender_meta_at(
        &self,
        body_off: u64,
        body_len: u64,
        vout: u32,
    ) -> Result<(bool, Fk, u32), StoreError> {
        let abs = spent_abs(body_off, vout);
        let end = body_off.saturating_add(body_len);
        if abs.saturating_add(OutputRecord::SPENT_SLOT_LEN as u64) > end {
            return Err(StoreError::Corrupt("spent slot OOB"));
        }
        if let Some(raw) = self.spend_pages.slot(abs)? {
            let (flags, field, vin) = decode_spent_slot(&raw)?;
            return Ok((flags & output_flags::MULTI_SPENDER != 0, field, vin));
        }
        self.spent
            .with_bytes_at(abs, OutputRecord::SPENT_SLOT_LEN as u64, |raw| {
                let (flags, field, vin) = decode_spent_slot(raw)?;
                Ok((flags & output_flags::MULTI_SPENDER != 0, field, vin))
            })
    }

    /// Spender meta for many vouts.
    ///
    /// The delta is read first; a slot it leaves to `spent.body` is read
    /// there, on a page a fold wrote. Returns `(vout, multi, field, vin)` for
    /// each vout inside the range; one past it is omitted.
    pub fn get_output_spender_metas_at(
        &self,
        body_off: u64,
        body_len: u64,
        vouts: &[u32],
    ) -> Result<Vec<(u32, bool, Fk, u32)>, StoreError> {
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        let slot = OutputRecord::SPENT_SLOT_LEN;
        let end = body_off.saturating_add(body_len);
        let mut hits: Vec<Option<(bool, Fk, u32)>> = vec![None; vouts.len()];
        let mut in_range: Vec<(usize, u64)> = Vec::new();
        for (i, &v) in vouts.iter().enumerate() {
            let abs = spent_abs(body_off, v);
            if abs.saturating_add(slot as u64) <= end {
                in_range.push((i, abs));
            }
        }
        let logged = self
            .spend_pages
            .slots_at(&in_range.iter().map(|(_, abs)| *abs).collect::<Vec<_>>())?;
        for (&(i, abs), raw) in in_range.iter().zip(logged) {
            let (field, flags, vin) = match raw {
                Some(logged) => {
                    let (flags, field, vin) = decode_spent_slot(&logged)?;
                    (field, flags, vin)
                }
                None => self.body_spend_slot(abs)?,
            };
            hits[i] = Some((flags & output_flags::MULTI_SPENDER != 0, field, vin));
        }
        Ok(Self::pack_spender_metas(vouts, hits))
    }

    fn pack_spender_metas(
        vouts: &[u32],
        hits: Vec<Option<(bool, Fk, u32)>>,
    ) -> Vec<(u32, bool, Fk, u32)> {
        vouts
            .iter()
            .zip(hits)
            .filter_map(|(&v, hit)| hit.map(|(multi, field, vin)| (v, multi, field, vin)))
            .collect()
    }

    pub(crate) fn note_spend_slots(
        &self,
        slots: &[(u64, [u8; OutputRecord::SPENT_SLOT_LEN])],
    ) -> Result<(), StoreError> {
        self.spend_pages.note_slots(slots)
    }

    /// The fold's `spent.body` sync. It shares the barrier's fd, so it runs
    /// under the Class A latch.
    #[doc(hidden)]
    pub fn sync_folded_spent(&self) -> Result<(), StoreError> {
        self.class_a_latched(|| self.spent.sync_data_only())
    }

    pub(crate) fn checkpoint_spend_pages<B>(
        &self,
        before_freeze: B,
        durable: Option<u32>,
    ) -> Result<(), StoreError>
    where
        B: FnOnce(&crate::spend_delta::SpendPageLog) -> Result<(), StoreError>,
    {
        self.spend_pages
            .checkpoint(before_freeze, &mut SpentFold::of(self), durable)
            .map(|_| ())
    }

    /// Lifetime write counters and sizes of the spend delta.
    pub fn spend_delta_stats(&self) -> crate::spend_delta::SpendDeltaStats {
        self.spend_pages.stats()
    }

    pub(crate) fn bind_spend_sync_wake(&self, wake: crate::spend_delta::SpendSyncWake) {
        self.spend_pages.bind_sync_wake(wake);
    }

    pub(crate) fn spend_checkpoint_early(&self) -> bool {
        self.spend_pages.checkpoint_early()
    }

    pub(crate) fn relax_spend_pace(&self) {
        self.spend_pages.relax_pace();
    }

    pub(crate) fn wait_spend_edge_room(&self) -> Result<(), StoreError> {
        self.spend_pages.wait_open_edge_room()
    }

    #[doc(hidden)]
    pub fn spend_log_slot(&self, abs: u64) -> Result<Option<[u8; 8]>, StoreError> {
        self.spend_pages.slot(abs)
    }

    pub(crate) fn drop_spends_above<K>(&self, tip: u32, keep: K) -> Result<(), StoreError>
    where
        K: FnMut(&[u8; OutputRecord::SPENT_SLOT_LEN]) -> Result<bool, StoreError>,
    {
        self.spend_pages.drop_spends_above(tip, keep)
    }

    /// Record one decided spend slot as a delta note. Class A writes no
    /// `spent.body` bytes; a fold is their only writer. A slot past the
    /// published body is refused.
    pub(crate) fn store_spend_slot_at(
        &self,
        body_off: u64,
        body_len: u64,
        vout: u32,
        multi: bool,
        field: Fk,
        vin: u32,
    ) -> Result<(), StoreError> {
        let abs = spent_abs(body_off, vout);
        let end = body_off.saturating_add(body_len);
        if abs.saturating_add(OutputRecord::SPENT_SLOT_LEN as u64) > end {
            return Err(StoreError::Corrupt("spent slot OOB"));
        }
        let flags = if multi {
            output_flags::MULTI_SPENDER
        } else {
            0
        };
        let slot_vin = if multi { 0 } else { vin };
        if !crate::spend_delta::is_historical(self.spent.body_published_len(), abs) {
            // Only `clip` at open drops a note past the end: a create that
            // failed to publish would leave it for the next create there.
            return Err(StoreError::Corrupt("spent slot past the published body"));
        }
        let meta = encode_spent_slot(flags, field, slot_vin)?;
        self.note_spend_slots(&[(abs, meta)])
    }

    /// Full tx: `txout` + `seqsigwit` zip.
    pub fn get_full(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        if self.prune_seqsigwit_mode() {
            return Err(StoreError::NotFound);
        }
        let pair = self
            .create_loc_range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)?;
        let raw = self
            .body
            .with_bytes_at(pair.txout.0, pair.txout.1, |b| Ok(b.to_vec()))?;
        let (mut tx, _ins, outs, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, pair.n_out, Some(&self.secret))?;
        let ir = self
            .seqsigwit_loc
            .range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)?;
        let seqsigwit = self
            .seqsigwit
            .with_bytes_at(ir.0, ir.1, |b| Ok(b.to_vec()))?;
        let mut ins = if tx.input_count == 0 {
            decode_seqsigwit_secret_to_end(&seqsigwit, Some(&self.secret))?
        } else {
            decode_seqsigwit_secret(&seqsigwit, tx.input_count, Some(&self.secret))?
        };
        self.stamp_seqsigwit_prevouts(fk, &mut ins)?;
        tx.input_count = ins.len() as u32;
        tx.txid = self.txids.get(fk)?;
        Ok((tx, ins, outs))
    }

    pub fn stamp_seqsigwit_prevouts(
        &self,
        fk: Fk,
        ins: &mut [InputRecord],
    ) -> Result<(), StoreError> {
        let Some(edges) = self.input.edges(fk)? else {
            return Ok(());
        };
        apply_input_edges(ins, &edges)
    }

    /// Contiguous create_fks `first..=last`: one libc span each of `txout.body`
    /// and `seqsigwit.body`, plus `txid.body` range. Not the confirm uring pipeline.
    pub fn get_full_span(&self, first: u64, last: u64) -> Result<Vec<PackedTx>, StoreError> {
        if self.prune_seqsigwit_mode() {
            return Err(StoreError::NotFound);
        }
        if first == 0 {
            return Err(StoreError::InvalidFk);
        }
        if last < first {
            return Ok(Vec::new());
        }
        let n = (last - first + 1) as usize;
        let fks: Vec<Fk> = (first..=last).map(Fk).collect();
        let loc_pairs = self.create_loc.range_batch(&fks)?;
        let mut txout_ranges = Vec::with_capacity(n);
        let mut n_outs = Vec::with_capacity(n);
        for p in loc_pairs {
            let p = p.ok_or(StoreError::NotFound)?;
            txout_ranges.push(p.txout);
            n_outs.push(p.n_out);
        }
        let seqsigwit_pairs = self.seqsigwit_loc.range_batch(&fks)?;
        let mut seqsigwit_ranges = Vec::with_capacity(n);
        for p in seqsigwit_pairs {
            seqsigwit_ranges.push(p.ok_or(StoreError::NotFound)?);
        }
        if txout_ranges.len() != n || seqsigwit_ranges.len() != n {
            return Err(StoreError::Corrupt("invariant: full span range count"));
        }
        let (t0, _) = txout_ranges[0];
        let (tn, tln) = txout_ranges[n - 1];
        let tspan = tn
            .checked_add(tln)
            .and_then(|end| end.checked_sub(t0))
            .ok_or(StoreError::Corrupt("txout span"))?;
        let (i0, _) = seqsigwit_ranges[0];
        let (inn, iln) = seqsigwit_ranges[n - 1];
        let ispan = inn
            .checked_add(iln)
            .and_then(|end| end.checked_sub(i0))
            .ok_or(StoreError::Corrupt("seqsigwit span"))?;
        let ids = self.txids.get_range(first, last)?;
        if ids.len() != n {
            return Err(StoreError::Corrupt("invariant: txid.body span length"));
        }
        let (txout_span, seqsigwit_span) =
            pread_two_spans(&self.body, t0, tspan, &self.seqsigwit, i0, ispan, true)?;
        let edge_rows = self.input.edges_span(first, last)?;
        if edge_rows.len() != n {
            return Err(StoreError::Corrupt("invariant: input span length"));
        }
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let (toff, tlen) = txout_ranges[i];
            let (ioff, ilen) = seqsigwit_ranges[i];
            let traw = span_rec(&txout_span, t0, toff, tlen)?;
            let iraw = span_rec(&seqsigwit_span, i0, ioff, ilen)?;
            let (mut tx, _ins, outs, _) =
                decode_packed_tx_with_spender_rels_secret(traw, n_outs[i], Some(&self.secret))?;
            let mut ins = if tx.input_count == 0 {
                decode_seqsigwit_secret_to_end(iraw, Some(&self.secret))?
            } else {
                decode_seqsigwit_secret(iraw, tx.input_count, Some(&self.secret))?
            };
            if let Some(edges) = edge_rows[i].as_ref() {
                apply_input_edges(&mut ins, edges)?;
            }
            tx.input_count = ins.len() as u32;
            tx.txid = ids[i];
            out.push((tx, ins, outs));
        }
        Ok(out)
    }

    /// Meta + outputs only (one body IO; skips input materialization).
    pub fn get_meta_and_outputs(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        let pair = self
            .create_loc_range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)?;
        let raw = self
            .body
            .with_bytes_at(pair.txout.0, pair.txout.1, |b| Ok(b.to_vec()))?;
        #[cfg(debug_assertions)]
        self.body_decodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (mut tx, outs, _) =
            decode_packed_tx_outs_with_spender_rels_secret(&raw, pair.n_out, Some(&self.secret))?;
        #[cfg(debug_assertions)]
        self.outputs_decoded
            .fetch_add(outs.len() as u64, std::sync::atomic::Ordering::Relaxed);
        tx.txid = self.txids.get(fk)?;
        self.overlay_stamped_n_in(fk, &mut tx)?;
        Ok((tx, outs))
    }

    /// The outputs at `vouts` of one create, in ascending vout order: one
    /// body read, and only those outputs decoded. The other outputs are
    /// skipped by length, not materialized, through the end of the row, so
    /// a row that does not decode as a whole is still corrupt here, even
    /// when nothing is asked of it. A vout at or past the output count is
    /// left out; a readable row with only such vouts reads as empty.
    pub fn get_outputs_at(
        &self,
        fk: Fk,
        vouts: &[u32],
    ) -> Result<Vec<(u32, OutputRecord)>, StoreError> {
        let pair = self
            .create_loc_range_batch(&[fk])?
            .into_iter()
            .next()
            .flatten()
            .ok_or(StoreError::NotFound)?;
        let mut need: Vec<u32> = vouts.iter().copied().filter(|&v| v < pair.n_out).collect();
        need.sort_unstable();
        need.dedup();
        let (_, live, _) = self.body.with_bytes_at(pair.txout.0, pair.txout.1, |raw| {
            decode_packed_tx_outs_exact(raw, pair.n_out, &need, Some(&self.secret))
        })?;
        #[cfg(debug_assertions)]
        self.body_decodes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        #[cfg(debug_assertions)]
        self.outputs_decoded
            .fetch_add(live.len() as u64, std::sync::atomic::Ordering::Relaxed);
        Ok(live)
    }

    /// Sample-and-reset per-fk packed body decodes (instance stats).
    #[cfg(debug_assertions)]
    pub fn sample_reset_body_decodes(&self) -> u64 {
        self.body_decodes
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Sample-and-reset output records materialized (instance stats).
    #[cfg(debug_assertions)]
    pub fn sample_reset_outputs_decoded(&self) -> u64 {
        self.outputs_decoded
            .swap(0, std::sync::atomic::Ordering::Relaxed)
    }

    /// Walk create_fks `first..=last` from a coalesced `txout.body` span (one idx
    /// walk, sequential body pread), yielding `script_hash` per out.
    ///
    /// Body IO is libc `pread` (not the TLS uring ring). Collect is nCPU workers
    /// each doing one large sequential span — not a completion machine. The ring
    /// 5 s wait is a lost-CQE fence for 4 KiB lookup/g-page waves.
    pub fn for_each_script_hashes_in_fk_span(
        &self,
        first: u64,
        last: u64,
        mut f: impl FnMut(Fk, [u8; 32]) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        if last < first {
            return Ok(());
        }
        let fks: Vec<Fk> = (first..=last).map(Fk).collect();
        let loc = match self.create_loc.range_batch(&fks) {
            Ok(r) => r,
            Err(StoreError::NotFound) | Err(StoreError::InvalidFk) => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut ranges = Vec::with_capacity(loc.len());
        let mut n_outs = Vec::with_capacity(loc.len());
        for p in loc {
            let Some(p) = p else {
                return Ok(());
            };
            ranges.push(p.txout);
            n_outs.push(p.n_out);
        }
        const MAX_SPAN: u64 = SCRIPT_HASH_COLLECT_SPAN;
        let mut i = 0usize;
        while i < ranges.len() {
            let span_lo = ranges[i].0;
            let mut span_hi = ranges[i].0.saturating_add(ranges[i].1);
            let mut j = i + 1;
            while j < ranges.len() {
                let (off, len) = ranges[j];
                if off != span_hi || span_hi.saturating_sub(span_lo).saturating_add(len) > MAX_SPAN
                {
                    break;
                }
                span_hi = span_hi.saturating_add(len);
                j += 1;
            }
            let span_len = span_hi.saturating_sub(span_lo);
            self.body.with_bytes_at_pread(span_lo, span_len, |buf| {
                for (k, &(off, len)) in ranges.iter().enumerate().take(j).skip(i) {
                    let rel = (off.saturating_sub(span_lo)) as usize;
                    let end = rel.saturating_add(len as usize);
                    if end > buf.len() {
                        return Err(StoreError::Corrupt("txout span short for fk range"));
                    }
                    let fk = Fk(first.saturating_add(k as u64));
                    visit_packed_script_hashes(
                        &buf[rel..end],
                        n_outs[k],
                        Some(&self.secret),
                        |sh| f(fk, sh),
                    )?;
                }
                Ok(())
            })?;
            i = j;
        }
        Ok(())
    }

    /// Sum of output values for each create in `first..=last`, in fk order.
    /// Adjacent `txout.body` ranges coalesce into reads of at most
    /// `span_max` bytes (one create's range is never split). A create in the
    /// span without a `create.loc` row is `Corrupt`: a short list would
    /// misalign with the block's txids.
    pub fn output_value_sums_in_fk_span(
        &self,
        first: u64,
        last: u64,
        span_max: u64,
    ) -> Result<Vec<u64>, StoreError> {
        if last < first {
            return Ok(Vec::new());
        }
        let fks: Vec<Fk> = (first..=last).map(Fk).collect();
        let mut locs = Vec::with_capacity(fks.len());
        for p in self.create_loc.range_batch(&fks)? {
            locs.push(p.ok_or(StoreError::Corrupt(
                "invariant: create.loc missing in value-sum span",
            ))?);
        }
        let mut sums = Vec::with_capacity(locs.len());
        let mut i = 0usize;
        while i < locs.len() {
            let span_lo = locs[i].txout.0;
            let mut span_hi = span_lo.saturating_add(locs[i].txout.1);
            let mut j = i + 1;
            while let Some(&(off, len)) = locs.get(j).map(|p| &p.txout) {
                if off != span_hi || span_hi.saturating_sub(span_lo).saturating_add(len) > span_max
                {
                    break;
                }
                span_hi = span_hi.saturating_add(len);
                j += 1;
            }
            self.body
                .with_bytes_at_pread(span_lo, span_hi.saturating_sub(span_lo), |buf| {
                    for p in &locs[i..j] {
                        let rel = p.txout.0.saturating_sub(span_lo) as usize;
                        let raw = buf
                            .get(rel..rel.saturating_add(p.txout.1 as usize))
                            .ok_or(StoreError::Corrupt("txout span short for fk range"))?;
                        sums.push(packed_output_value_sum(raw, p.n_out)?);
                    }
                    Ok(())
                })?;
            i = j;
        }
        Ok(sums)
    }

    /// Append Class A rows: `txout` + `seqsigwit` + zero `spent` + `txid.body` + `txstat.body`.
    pub fn put_full_batch_indexed(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        let rows = txstat_placeholders(items);
        self.put_full_batch_indexed_with_txstat(items, index, &rows)
    }

    pub fn put_full_batch_indexed_with_txstat(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
        txstat: &[crate::txstat::TxStatRow],
    ) -> Result<Vec<Fk>, StoreError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        self.refuse_if_class_a_failed()?;
        if txstat.len() != items.len() {
            return Err(StoreError::Corrupt("txstat batch length"));
        }
        let est_out: usize = items
            .iter()
            .map(|(_tx, _ins, outs)| {
                16 + TxRecord::BODY_META_LEN + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let est_seqsigwit: usize = items
            .iter()
            .map(|(_tx, ins, _outs)| 16 + ins.iter().map(|i| i.encoded_len()).sum::<usize>())
            .sum();
        let base = self.body.count();
        if (!self.prune_seqsigwit_mode() && self.seqsigwit.count() != base)
            || self.spent.count() != base
            || self.txstat.count() != base
            || self.input.count() != base
        {
            return Err(StoreError::Corrupt("Class A stem count mismatch on append"));
        }
        if items.iter().any(|(_, _, outs)| outs.is_empty()) {
            return Err(StoreError::Corrupt("invariant: create n_out"));
        }
        let n_outs: Vec<u32> = items.iter().map(|(_, _, o)| o.len() as u32).collect();
        let fks = self.class_a_append(|| {
            let spent = self.spent.prepare_sized(&spent_lens(&n_outs))?;
            let (fks, _loc) = self.append_stems_one_wave(
                items.len(),
                est_out,
                est_seqsigwit,
                &n_outs,
                |i, buf| {
                    let (tx, ins, outs) = &items[i];
                    encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
                },
                |i, buf| encode_seqsigwit_with_secret(&items[i].1, buf, Some(&self.secret)),
                spent,
            )?;
            run_after_bodies_hook();
            let ids: Vec<[u8; 32]> = items.iter().map(|(tx, _, _)| tx.txid).collect();
            self.txids.append_batch(base, &ids)?;
            let tails = self.txstat.append_batch(base, txstat)?;
            if !tails.is_empty() {
                return Err(StoreError::Corrupt("txstat overflow needs header blob"));
            }
            let edges: Vec<Vec<crate::input::InputEdge>> =
                items.iter().map(|(_, ins, _)| input_edges(ins)).collect();
            self.input.append(&edges)?;
            Ok(fks)
        })?;
        self.settle(base, items.len());
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

    /// Like [`Self::put_full_batch_indexed`], but outs live in a shared pin
    /// ([`PackedCreate`]). Encode borrows pin fields — no outs deep clone.
    pub fn put_full_batch_from_pins<P: PackedCreate>(
        &self,
        items: &[(P, Vec<InputRecord>)],
        index: bool,
    ) -> Result<(Vec<Fk>, Vec<crate::create_loc::CreateLocPair>), StoreError> {
        let rows: Vec<crate::txstat::TxStatRow> =
            items.iter().map(|_| txstat_placeholder()).collect();
        self.put_full_batch_from_pins_with_txstat(items, index, &rows, &[])
    }

    pub fn put_full_batch_from_pins_with_txstat<P: PackedCreate>(
        &self,
        items: &[(P, Vec<InputRecord>)],
        index: bool,
        txstat: &[crate::txstat::TxStatRow],
        header_ranges: &[(Fk, Fk, u32)],
    ) -> Result<(Vec<Fk>, Vec<crate::create_loc::CreateLocPair>), StoreError> {
        let pins: Vec<&P> = items.iter().map(|(pin, _)| pin).collect();
        let edges: Vec<Vec<crate::input::InputEdge>> =
            items.iter().map(|(_, ins)| input_edges(ins)).collect();
        let est_seqsigwit: usize = items
            .iter()
            .map(|(_, ins)| 16 + ins.iter().map(|i| i.encoded_len()).sum::<usize>())
            .sum();
        let secret = self.secret.clone();
        self.put_pins_with_edges(
            &pins,
            index,
            txstat,
            header_ranges,
            &edges,
            est_seqsigwit,
            |i, buf| encode_seqsigwit_with_secret(&items[i].1, buf, Some(&secret)),
        )
    }

    #[allow(clippy::too_many_arguments)] // same wave as append_stems_one_wave
    /// Class A append from pins. `input_edges[i]` is `(parent_fk, vout)` per vin.
    /// A null parent is coinbase (`vout` ignored). `encode_in` writes that row's
    /// `seqsigwit` bytes, XOR included when a secret applies.
    pub fn put_pins_encoded<P: PackedCreate>(
        &self,
        pins: &[&P],
        index: bool,
        txstat: &[crate::txstat::TxStatRow],
        header_ranges: &[(Fk, Fk, u32)],
        input_edges: &[Vec<crate::input::InputEdge>],
        est_seqsigwit: usize,
        encode_in: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<(Vec<Fk>, Vec<crate::create_loc::CreateLocPair>), StoreError> {
        self.put_pins_with_edges(
            pins,
            index,
            txstat,
            header_ranges,
            input_edges,
            est_seqsigwit,
            encode_in,
        )
    }

    #[allow(clippy::too_many_arguments)] // same wave as append_stems_one_wave
    #[doc(hidden)]
    pub fn put_pins_with_edges<P: PackedCreate>(
        &self,
        pins: &[&P],
        index: bool,
        txstat: &[crate::txstat::TxStatRow],
        header_ranges: &[(Fk, Fk, u32)],
        input_edges: &[Vec<crate::input::InputEdge>],
        est_seqsigwit: usize,
        encode_in: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<(Vec<Fk>, Vec<crate::create_loc::CreateLocPair>), StoreError> {
        if pins.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }
        self.refuse_if_class_a_failed()?;
        if txstat.len() != pins.len() {
            return Err(StoreError::Corrupt("txstat batch length"));
        }
        if input_edges.len() != pins.len() {
            return Err(StoreError::Corrupt("input edge batch length"));
        }
        let est_out: usize = pins.iter().map(|pin| pin.packed_outs_est()).sum();
        let base = self.body.count();
        if (!self.prune_seqsigwit_mode() && self.seqsigwit.count() != base)
            || self.spent.count() != base
            || self.txstat.count() != base
            || self.input.count() != base
        {
            return Err(StoreError::Corrupt("Class A stem count mismatch on append"));
        }
        let mut n_outs = Vec::with_capacity(pins.len());
        for pin in pins {
            let n_out = pin.packed_n_out();
            if n_out == 0 {
                return Err(StoreError::Corrupt("invariant: create n_out"));
            }
            n_outs.push(n_out);
        }
        let (fks, loc) = self.class_a_append(|| {
            let spent = self.spent.prepare_sized(&spent_lens(&n_outs))?;
            let (fks, loc) = self.append_stems_one_wave(
                pins.len(),
                est_out,
                est_seqsigwit,
                &n_outs,
                |i, buf| {
                    pins[i].encode_txout_body(buf, Some(&self.secret));
                },
                encode_in,
                spent,
            )?;
            run_after_bodies_hook();
            let ids: Vec<[u8; 32]> = pins.iter().map(|pin| pin.packed_txid()).collect();
            self.txids.append_batch(base, &ids)?;
            let tails = self.txstat.append_batch(base, txstat)?;
            self.txstat
                .put_overflows_for_headers(header_ranges, &tails)?;
            self.input.append(input_edges)?;
            Ok((fks, loc))
        })?;
        self.settle(base, pins.len());
        if index {
            let heads: Vec<([u8; 32], Fk)> = pins
                .iter()
                .zip(fks.iter())
                .map(|(pin, fk)| (pin.packed_txid(), *fk))
                .collect();
            self.head_insert_many(&heads)?;
        }
        Ok((fks, loc))
    }

    /// Arm short pwrites on one Class A body (`txout` or `seqsigwit`;
    /// Class A writes no `spent` bytes).
    ///
    /// The next long writes to that file store an 8-byte prefix and return
    /// disk-full, and they stay failing until [`Self::disarm_short_body_pwrite`].
    /// Lengths are unpublished until every payload write has succeeded.
    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn arm_short_body_pwrite(&self, stem: &str) {
        let handle = match stem {
            "txout" => self.body.body_write_fd(),
            "seqsigwit" => self.seqsigwit.body_write_fd(),
            _ => panic!("arm_short_body_pwrite: unknown stem {stem}"),
        };
        crate::io_handle::arm_short_pwrite(handle);
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn disarm_short_body_pwrite(&self) {
        crate::io_handle::disarm_short_pwrite();
    }

    #[allow(clippy::too_many_arguments)] // IO/session args stay unbundled
    /// Encode and write `txout` + `seqsigwit` + `spent` bodies as one pwrite wave.
    ///
    /// Payloads land past the published ends. Body headers are persisted
    /// before the locator header of the same commit. Not the spend-annotate
    /// machine.
    #[doc(hidden)]
    pub fn append_stems_one_wave(
        &self,
        n: usize,
        est_out: usize,
        est_seqsigwit: usize,
        n_outs: &[u32],
        encode_out: impl FnMut(usize, &mut Vec<u8>),
        encode_in: impl FnMut(usize, &mut Vec<u8>),
        p_sp: crate::var_table::PreparedAppend,
    ) -> Result<(Vec<Fk>, Vec<crate::create_loc::CreateLocPair>), StoreError> {
        if n_outs.len() != n || p_sp.starts.len() != n {
            return Err(StoreError::Corrupt("invariant: create n_out"));
        }
        let Some(p_out) = self.body.prepare_batch_encode(n, est_out, encode_out)? else {
            return Ok((Vec::new(), Vec::new()));
        };
        let (recs, loc) = class_a_loc_pairs(n, n_outs, &p_out, &p_sp);
        let staged_loc = self.create_loc.stage(&recs)?;
        let fks = if self.prune_seqsigwit_mode() {
            self.commit_pruned_wave(p_out, p_sp, staged_loc)?
        } else {
            self.commit_seq_wave(n, est_seqsigwit, encode_in, p_out, p_sp, staged_loc)?
        };
        Ok((fks, loc))
    }

    /// A body or locator header pwrite that failed is an append that failed:
    /// RAM names creates the on-disk headers do not.
    fn headers_landed(&self, landed: bool) -> Result<(), StoreError> {
        if landed {
            return Ok(());
        }
        Err(StoreError::io(
            self.create_loc.loc.path(),
            std::io::Error::other("a Class A header write failed"),
        ))
    }

    fn commit_pruned_wave(
        &self,
        p_out: crate::var_table::PreparedAppend,
        p_sp: crate::var_table::PreparedAppend,
        staged_loc: crate::create_loc::StagedCreate,
    ) -> Result<Vec<Fk>, StoreError> {
        crate::var_table::write_prepared_bodies_one_wave(
            &[(&self.body, &p_out)],
            self.datadir_hdd(),
        )?;
        self.create_loc.write_staged(&staged_loc)?;
        let (fks, bodies) = publish_body_pair(&self.body, p_out, &self.spent, p_sp, true)?;
        let landed = self.create_loc.commit_staged(staged_loc, bodies);
        self.headers_landed(landed)?;
        Ok(fks)
    }

    fn commit_seq_wave(
        &self,
        n: usize,
        est_seqsigwit: usize,
        encode_in: impl FnMut(usize, &mut Vec<u8>),
        p_out: crate::var_table::PreparedAppend,
        p_sp: crate::var_table::PreparedAppend,
        staged_loc: crate::create_loc::StagedCreate,
    ) -> Result<Vec<Fk>, StoreError> {
        let Some(p_in) = self
            .seqsigwit
            .prepare_batch_encode(n, est_seqsigwit, encode_in)?
        else {
            return Err(StoreError::Corrupt("Class A seqsigwit prepare empty"));
        };
        let seq_lens = p_in.aligned_lens();
        let staged_seq = self.seqsigwit_loc.stage(&p_in.starts, &seq_lens)?;
        crate::var_table::write_prepared_bodies_one_wave(
            &[(&self.body, &p_out), (&self.seqsigwit, &p_in)],
            self.datadir_hdd(),
        )?;
        self.create_loc.write_staged(&staged_loc)?;
        self.seqsigwit_loc.write_staged(&staged_seq)?;
        let (fks_in, in_ok) = publish_prepared_body(&self.seqsigwit, p_in, true)?;
        let (fks, pair_ok) = publish_body_pair(&self.body, p_out, &self.spent, p_sp, in_ok)?;
        expect_same_fks(&fks, &fks_in)?;
        let durable = self.create_loc.commit_staged(staged_loc, in_ok && pair_ok);
        let landed = self.seqsigwit_loc.commit_staged(staged_seq, durable);
        self.headers_landed(landed)?;
        Ok(fks)
    }

    pub fn get_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        let Some(fk) = self.probe_body_match_fk(txid)? else {
            return Ok(None);
        };
        Ok(Some((fk, self.get(fk)?)))
    }

    /// All Class A fks whose body txid equals `txid` (BIP30: more than one).
    ///
    /// Order is **newest-first** (deepest probe match first), matching
    /// [`Self::probe_body_match_fk`]. Verifies `txid.body` only; no packed
    /// decode.
    pub fn fks_by_txid(&self, txid: &[u8; 32]) -> Result<Vec<Fk>, StoreError> {
        let mut out: Vec<Fk> = Vec::new();
        let mixed = self.secret.mix_txid(txid);
        let cands = self.head.probe_candidates(&mixed)?;
        for fk in cands {
            if out.contains(&fk) || self.body_txid(fk)? != *txid {
                continue;
            }
            out.push(fk);
        }
        Ok(out)
    }

    /// Annotate many vouts on one create. `spent_off`/`spent_len` are the
    /// `spent.body` range (not `txout`).
    pub fn put_spends_on_create_at(
        &self,
        spenders: &crate::spender_table::SpenderTable,
        spent_off: u64,
        spent_len: u64,
        edges: &[(u32, Fk, u32)],
    ) -> Result<(), StoreError> {
        if edges.is_empty() {
            return Ok(());
        }
        for &(_, sfk, _) in edges {
            if sfk.is_null() {
                return Err(StoreError::InvalidFk);
            }
        }
        for &(vout, spend_fk, spend_vin) in edges {
            let (multi, field, field_vin) =
                self.get_output_spender_meta_at(spent_off, spent_len, vout)?;
            let (new_multi, new_field, new_vin) = if !multi && field.is_null() {
                (false, spend_fk, spend_vin)
            } else if !multi && field == spend_fk && field_vin == spend_vin {
                continue;
            } else if !multi {
                let e1 = spenders.append(field, field_vin, Fk::NULL)?;
                let e2 = spenders.append(spend_fk, spend_vin, e1)?;
                (true, e2, 0)
            } else {
                let e = spenders.append(spend_fk, spend_vin, field)?;
                (true, e, 0)
            };
            self.store_spend_slot_at(spent_off, spent_len, vout, new_multi, new_field, new_vin)?;
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
        if self.head.occupied() == 0 && self.count() > 0 {
            return self.rebuild_head_from_bodies(on_progress);
        }
        self.backfill_head_inner(/* force_all */ false, on_progress)
    }

    /// Insert `txid.body` → `tx.head` for creates `first_fk..=count` (no presence probe).
    pub fn backfill_head_from(&self, first_fk: u64) -> Result<u64, StoreError> {
        let n = self.count();
        if first_fk == 0 || first_fk > n {
            return Ok(0);
        }
        let mut inserted = 0u64;
        let read_batch: u64 = 65_536;
        let write_chunk: usize = 65_536;
        let mut batch: Vec<([u8; 32], Fk)> = Vec::with_capacity(write_chunk);
        let mut cur = first_fk;
        let live = rbitcoin_log::progress::begin_at("tx.head backfill", n, first_fk - 1);
        while cur <= n {
            let end = (cur + read_batch - 1).min(n);
            let txids = self.body_txid_range(cur, end)?;
            for (i, txid) in txids.into_iter().enumerate() {
                batch.push((txid, Fk(cur + i as u64)));
                if batch.len() >= write_chunk {
                    inserted += batch.len() as u64;
                    self.head_insert_many(&batch)?;
                    batch.clear();
                    live.set_done(first_fk - 1 + inserted);
                }
            }
            cur = end + 1;
        }
        if !batch.is_empty() {
            inserted += batch.len() as u64;
            self.head_insert_many(&batch)?;
            live.set_done(first_fk - 1 + inserted);
        }
        Ok(inserted)
    }

    /// Rebuild sealed MPHF+fuse8 from Class A (`txid.body`), no historical OA.
    ///
    /// Range width is [`Self::rebuild_seal_keys`] (default 2²⁵). Remainder is
    /// sealed too; an empty open tail is created for later inserts.
    /// Workers: [`Self::rebuild_workers`] (min of CPUs, free RAM / 1 GiB,
    /// and range count). Distinct from SH extract's 1.5 GiB cap.
    pub fn rebuild_head_from_bodies(
        &self,
        mut on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, StoreError> {
        let n = self.count();
        if n == 0 {
            return Ok(0);
        }
        let ranges = self.plan_head_rebuild_ranges()?;
        let n_jobs = ranges.len();
        let workers = self.rebuild_workers().min(n_jobs).max(1);
        let seal_bits = self.rebuild_seal_bits();
        rbitcoin_log::info!(
            "store: tx.head rebuild mphf n={n} seal_bits={seal_bits} ranges={n_jobs} \
             workers={workers} free_GiB={}",
            crate::free_gib_label()
        );
        let live = rbitcoin_log::progress::begin("tx.head rebuild", n);
        let jobs: Vec<(u32, u64, u64)> = ranges
            .into_iter()
            .enumerate()
            .map(|(i, (first, count))| (i as u32, first, count))
            .collect();
        let next = std::sync::atomic::AtomicUsize::new(0);
        type SealSlot = std::sync::Mutex<
            Option<Result<(u64, u64, crate::segmented_head::SealPublish), StoreError>>,
        >;
        let slots: Vec<SealSlot> = (0..n_jobs).map(|_| std::sync::Mutex::new(None)).collect();
        std::thread::scope(|scope| {
            for _ in 0..workers {
                let jobs = &jobs;
                let next = &next;
                let slots = &slots;
                scope.spawn(|| loop {
                    let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    if i >= n_jobs {
                        break;
                    }
                    let (file_id, first, count) = jobs[i];
                    let r = self.seal_rebuild_range(file_id, first, count);
                    if r.is_ok() {
                        live.add_done(count);
                    }
                    *slots[i].lock().unwrap_or_else(|e| e.into_inner()) = Some(r);
                });
            }
        });
        let mut sealed = Vec::with_capacity(n_jobs);
        let mut inserted = 0u64;
        for cell in slots {
            let (first, count, pubd) = cell
                .into_inner()
                .unwrap_or_else(|e| e.into_inner())
                .ok_or(StoreError::Corrupt("tx.head rebuild worker silent"))??;
            inserted += count;
            on_progress(first.saturating_add(count).saturating_sub(1), n, inserted);
            sealed.push((first, count, pubd));
        }
        self.head
            .install_rebuild_sealed(sealed, n.saturating_add(1))?;
        Ok(inserted)
    }

    #[doc(hidden)]
    pub fn seal_rebuild_range(
        &self,
        file_id: u32,
        first: u64,
        count: u64,
    ) -> Result<(u64, u64, crate::segmented_head::SealPublish), StoreError> {
        if count == 0 {
            return Err(StoreError::Corrupt("tx.head rebuild empty range"));
        }
        let pairs = self.fuse_pairs_for_range(first, count)?;
        let pubd = self.head.write_sealed_pairs(file_id, first, count, pairs)?;
        Ok((first, count, pubd))
    }

    /// Parallel wipe-rebuild workers captured at open (env or explicit opts).
    pub fn rebuild_workers(&self) -> usize {
        self.rebuild_workers
    }

    /// Rebuild MPHF range width captured at open: `2^bits` keys.
    pub fn rebuild_seal_bits(&self) -> u32 {
        self.rebuild_seal_bits
    }
}

/// Class A cuts for a cold MPHF rebuild (`2^bits` keys, last range short).
pub fn plan_rebuild_ranges(n: u64, seal_bits: u32) -> Vec<(u64, u64)> {
    if n == 0 {
        return Vec::new();
    }
    let t = (1u64 << seal_bits.clamp(6, 26)).min(u64::from(u32::MAX));
    let mut out = Vec::new();
    let mut first = 1u64;
    while first <= n {
        let count = (n - first + 1).min(t);
        out.push((first, count));
        first += count;
    }
    out
}

impl TxTable {
    /// Class A cuts for a cold MPHF rebuild (`2^bits` keys, last range short).
    ///
    /// Independent of live OA 80% load.
    pub fn plan_head_rebuild_ranges(&self) -> Result<Vec<(u64, u64)>, StoreError> {
        Ok(plan_rebuild_ranges(self.count(), self.rebuild_seal_bits()))
    }

    #[doc(hidden)]
    pub fn backfill_head_inner(
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

    /// Highest create fk in `tx.head` (0 if empty).
    pub fn head_last_inserted_fk(&self) -> u64 {
        self.head.last_inserted_fk()
    }

    /// See [`crate::segmented_head::SegmentedTxHead::unsynced_first_fk`].
    pub fn head_unsynced_first_fk(&self) -> u64 {
        self.head.unsynced_first_fk()
    }

    /// Per-segment first create_fk (winner-age stats).
    pub fn head_first_fks_snapshot(&self) -> Vec<u64> {
        self.head.first_fks_snapshot()
    }

    pub fn head_bits(&self) -> u32 {
        self.head.bits()
    }

    pub fn head_slots(&self) -> u64 {
        self.head.slots()
    }

    pub fn head_segment_count(&self) -> usize {
        self.head.segment_count()
    }

    /// Queue txid→fk for durable `tx.head` drain (write-local list only).
    pub fn head_note_pending(&self, entries: &[([u8; 32], Fk)]) {
        self.pending_head.note(entries);
    }

    /// Write-local drain list lookup (same-batch spend annotate before insert).
    pub fn queued_pending_fk(&self, txid: &[u8; 32]) -> Option<Fk> {
        self.pending_head.queued_fk(txid)
    }

    /// Take the drain list on the write thread (drain receives an owned Vec).
    pub fn take_pending_queued(&self) -> Vec<([u8; 32], Fk)> {
        self.pending_head.take_queued()
    }

    /// Insert a taken drain list. Leftover identity is load-owned, not here.
    pub fn head_insert_queued(&self, batch: &[([u8; 32], Fk)]) -> Result<u64, StoreError> {
        if batch.is_empty() {
            return Ok(0);
        }
        self.head_insert_many(batch)?;
        Ok(batch.len() as u64)
    }

    /// Drain the pending insert queue via page-grouped [`Self::head_insert_many`].
    pub fn head_drain_pending(&self) -> Result<u64, StoreError> {
        let batch = self.take_pending_queued();
        self.head_insert_queued(&batch)
    }

    pub fn pending_head_len(&self) -> usize {
        self.pending_head.len()
    }

    pub fn pending_head_is_full(&self) -> bool {
        self.pending_head.len() >= PENDING_HEAD_CAP
    }

    /// Insert txid→fk into the segmented head (mixes keys; may seal/roll).
    ///
    /// Rolls the open tail when its fk span reaches 80% slots (`max_keys`).
    /// Class A loc/body size does not cut `tx.head` shards.
    pub fn head_insert_many(&self, entries: &[([u8; 32], Fk)]) -> Result<(), StoreError> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut mixed: Vec<([u8; 32], Fk)> = entries
            .iter()
            .map(|(txid, fk)| (self.secret.mix_txid(txid), *fk))
            .collect();
        self.head.insert_many_with(
            &mut mixed,
            Arc::new({
                let secret = self.secret.clone();
                let fd = self.txids.body_read_fd();
                let path = self.txids.file_path().to_path_buf();
                move |first_fk, count| {
                    TxTable::fuse_pairs_from_txid_pread(fd, &path, &secret, first_fk, count)
                }
            }),
        )
    }

    pub fn head_resize_size_snapshot(&self) -> HeadResizeSizeSnapshot {
        let n = self.count();
        let bits = self.head.bits();
        let slots = self.head.slots();
        let occ = self.head.occupied();
        let body_bytes = slots.saturating_mul(crate::address_head::ENTRY_BYTES);
        HeadResizeSizeSnapshot {
            class_a_n: n,
            primary_bits: bits,
            primary_slots: slots,
            primary_occupied: occ,
            primary_body_bytes: body_bytes,
            segment_count: self.head.segment_count() as u64,
            sealed_segments: self.head.sealed_segment_count() as u64,
            fuse8_bytes: self.head.sealed_fuse_resident_bytes(),
            mphf_g_bytes: self.head.sealed_mphf_g_resident_bytes(),
            head_map_bytes: self.head.open_map_bytes(),
            mphf_occ_bytes: 0,
            class_c_l2_bytes: 0,
        }
    }

    /// Flush segmented heads only.
    pub fn flush_head(&self) -> Result<(), StoreError> {
        self.head.flush()
    }

    /// Publish every Class A high-water mark and sync, then `tx.head`.
    ///
    /// These are the barrier's fds, so the flush runs under the Class A
    /// latch: `TableFile::flush` retries a failed sync, and a retry can
    /// return 0 over pages the kernel dropped.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.class_a_latched(|| self.flush_class_a_files())?;
        self.head.flush()?;
        Ok(())
    }

    /// [`Self::flush`] for `input` alone: the backfill and the prune release.
    pub fn flush_input(&self) -> Result<(), StoreError> {
        self.class_a_latched(|| self.input.flush())
    }

    fn flush_class_a_files(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.seqsigwit.flush()?;
        self.spent.flush()?;
        self.txids.flush()?;
        self.txstat.flush()?;
        self.input.flush()?;
        Ok(())
    }

    /// Device barrier for every Class A stem a commit names: the bodies, both
    /// locator families, `txid.body`, `txstat`, and `input`. Not `tx.head`.
    /// Does not publish high-water marks; headers already written are in
    /// the file and land with it.
    ///
    /// A failure latches: every later call refuses until restart. Calls run
    /// one at a time (the tip barrier and the spend checkpoint can overlap).
    pub fn sync_class_a_data(&self) -> Result<(), StoreError> {
        self.class_a_latched(|| {
            self.sync_class_a_stems(self.synced_named.load(std::sync::atomic::Ordering::Acquire))
        })
    }

    /// Call `on_fail` once, when a Class A sync or append write first fails.
    pub fn on_class_a_failure(&self, on_fail: impl Fn() + Send + Sync + 'static) {
        let _ = self.class_a_on_fail.set(Box::new(on_fail));
    }

    /// [`Self::sync_class_a_data`] for a record that names creates through
    /// `named` without a `header_txs` range (a spend delta commit, the
    /// scripthash `DONE.keys` / `DONE.post` marks, `include_hwm`). Open
    /// keeps every create through the highest mark.
    ///
    /// `named` is at most [`Self::settled_count`]: every row of those
    /// creates was written, and a failed write latched before this sync.
    pub fn sync_class_a_named(&self, named: u64) -> Result<(), StoreError> {
        use std::sync::atomic::Ordering;
        self.class_a_latched(|| {
            self.sync_class_a_stems(named)?;
            // Synced first, then named: the mark never passes a create the
            // device may not hold. One writer at a time under the latch.
            if named > self.synced_named.load(Ordering::Acquire) {
                crate::file::write_synced_tmp_rename(
                    &self.hot_dir.join(CLASS_A_SYNCED_NAME),
                    &named.to_le_bytes(),
                )?;
                self.synced_named.store(named, Ordering::Release);
            }
            Ok(())
        })
    }

    /// Run `sync` on Class A fds under the latch: refuse once one failed,
    /// one at a time, and latch a failure.
    fn class_a_latched(
        &self,
        sync: impl FnOnce() -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.class_a_latched_if(sync, |_| true)
    }

    /// A stats stamp after the barrier (`getblockstats` fills unstamped
    /// rows). It writes and syncs `txstat` files the barrier syncs, so it
    /// runs under the Class A latch, and a failed write or sync latches. A
    /// stamp refused before any IO (a create past the count) does not.
    pub(crate) fn stamp_txstat(
        &self,
        stamp: impl FnOnce(&crate::txstat::TxStat) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        self.class_a_latched_if(
            || stamp(&self.txstat),
            |e| matches!(e, StoreError::Io { .. }),
        )
    }

    fn class_a_latched_if(
        &self,
        sync: impl FnOnce() -> Result<(), StoreError>,
        latches: impl FnOnce(&StoreError) -> bool,
    ) -> Result<(), StoreError> {
        let _one = self.class_a_sync.lock().unwrap_or_else(|e| e.into_inner());
        self.refuse_if_class_a_failed()?;
        let synced = sync();
        if synced.as_ref().is_err_and(latches) {
            self.latch_class_a_failure();
        }
        synced
    }

    fn refuse_if_class_a_failed(&self) -> Result<(), StoreError> {
        if self
            .class_a_failed
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(StoreError::io(
                self.body.body.path(),
                std::io::Error::other("an earlier Class A write or sync failed; restart the node"),
            ));
        }
        Ok(())
    }

    fn latch_class_a_failure(&self) {
        let first = !self
            .class_a_failed
            .swap(true, std::sync::atomic::Ordering::AcqRel);
        if first {
            if let Some(on_fail) = self.class_a_on_fail.get() {
                on_fail();
            }
        }
    }

    /// Run the writes of one Class A append. A failed write latches: the
    /// append's rows may be in RAM and not on the device, so no barrier or
    /// mark may name them, and no later append may stack on them.
    fn class_a_append<T>(
        &self,
        write: impl FnOnce() -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        let written = write();
        if written.is_err() {
            self.latch_class_a_failure();
        }
        written
    }

    fn sync_class_a_stems(&self, named: u64) -> Result<(), StoreError> {
        let files: [&crate::file::TableFile; 16] = [
            &self.body.body,
            &self.seqsigwit.body,
            &self.spent.body,
            &self.create_loc.loc,
            &self.create_loc.ovf,
            &self.create_loc.off,
            &self.seqsigwit_loc.loc,
            &self.seqsigwit_loc.ovf,
            &self.seqsigwit_loc.off,
            &self.txids.file,
            &self.txstat.body,
            &self.txstat.ovf,
            &self.txstat.blk,
            &self.input.loc,
            &self.input.off,
            &self.input.body,
        ];
        crate::class_a_pages::write_barrier(&self.hot_dir, &files, named)
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.seqsigwit.flush_async()?;
        self.spent.flush_async()?;
        self.head.flush_async()?;
        Ok(())
    }
}
