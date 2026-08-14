use crate::address_head::HeadLayout;
use crate::compact::{
    input_flags, output_flags, read_compact_size, read_uleb128, write_compact_size, write_uleb128,
};
use crate::error::StoreError;
use crate::segmented_head::SegmentedTxHead;
use crate::var_table::VarTable;
use rbitcoin_primitives::{Fk, TableKind};
use std::path::Path;

/// Class A tx row (no wire blob — reconstruct from txout + inwit).
///
/// On-disk `txout.body` (schema **15**): **16 B meta** then outputs (no spender).
/// Identity lives in [`crate::txid_body::TxidBody`]. `txid` is filled in-memory
/// from the sidefile (or caller) after decode. `input_start_fk` / `output_start_fk`
/// stay [`Fk::NULL`] in RAM (legacy split-run address unused).
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
    /// On-disk `txout` meta length (schema 15: version, locktime, counts).
    pub const BODY_META_LEN: usize = 4 + 4 + 4 + 4; // 16
    /// Full in-memory encode size (txid + body meta); used for estimates only.
    pub const ENCODED_LEN: usize = 32 + Self::BODY_META_LEN;

    /// Encode full record including txid (tests / soft buffers — **not** Class A body).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::ENCODED_LEN);
        out.extend_from_slice(&self.txid);
        self.encode_body_meta_into(out);
    }

    /// Encode `txout` body meta (schema 15: no I/O fks).
    pub fn encode_body_meta_into(&self, out: &mut Vec<u8>) {
        out.reserve(Self::BODY_META_LEN);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.locktime.to_le_bytes());
        out.extend_from_slice(&self.input_count.to_le_bytes());
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

    /// Decode `txout` body meta (schema 15); `txid` left zero for caller fill.
    pub fn decode_body_meta(buf: &[u8]) -> Result<Self, StoreError> {
        if buf.len() < Self::BODY_META_LEN {
            return Err(StoreError::Corrupt("short tx body meta"));
        }
        Ok(Self {
            txid: [0u8; 32],
            version: i32::from_le_bytes(buf[0..4].try_into().unwrap()),
            locktime: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            input_start_fk: Fk::NULL,
            input_count: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            output_start_fk: Fk::NULL,
            output_count: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
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

    /// Encode `txout` payload (schema 15: **no** spender bytes; those live in `spent.body`).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let mut flags = 0u8;
        if self.script.is_empty() {
            flags |= output_flags::EMPTY_SCRIPT;
        } else if self.script == [0x51] {
            flags |= output_flags::OP_TRUE;
        }
        out.push(flags);
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
        let mut out = Vec::with_capacity(1 + 10 + 9 + self.script.len());
        self.encode_into(&mut out);
        out
    }

    /// Decode one `txout` output; spender fields are left null (load from `spent.body`).
    pub fn decode_at(buf: &[u8]) -> Result<(Self, usize), StoreError> {
        if buf.is_empty() {
            return Err(StoreError::Corrupt("short output record"));
        }
        let flags = buf[0];
        if flags & output_flags::MULTI_SPENDER != 0 {
            return Err(StoreError::Corrupt(
                "txout output must not carry MULTI_SPENDER",
            ));
        }
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
        let mut off = 1usize;
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
        1 + 10 + 9 + self.script.len()
    }

    /// Exact on-wire length matching [`Self::encode_into`].
    #[inline]
    pub fn encoded_len_exact(&self) -> usize {
        use crate::compact::{compact_size_len, uleb128_len};
        let v = if self.value < 0 {
            0u64
        } else {
            self.value as u64
        };
        let mut n = 1 + uleb128_len(v);
        if self.script.is_empty() || self.script == [0x51] {
        } else {
            n += compact_size_len(self.script.len() as u64) + self.script.len();
        }
        n
    }

    /// Sole-spender slot length in `spent.body`.
    pub const SPENT_SLOT_LEN: usize = 9;
}

mod packed;
mod pending_head;
pub use packed::*;
pub(crate) use pending_head::PENDING_HEAD_CAP;

pub struct TxTable {
    /// `txout.body` — meta + outputs (hot).
    pub(crate) body: VarTable,
    /// `inwit.body` — inputs + witness (cold).
    pub(crate) inwit: VarTable,
    /// `spent.body` — 9 B × n_out sole-spender slots.
    pub(crate) spent: VarTable,
    /// Segmented fixed-bits heads + seal-time fuse8.
    pub(crate) head: SegmentedTxHead,
    /// Dense create_fk-ordered txids (schema 13+).
    pub(crate) txids: crate::txid_body::TxidBody,
    /// Datadir secret: keyed head probes + script XOR (schema 12+).
    pub(crate) secret: crate::store_secret::StoreSecret,
    /// Unflushed head inserts (write-behind). Readers see published snapshot.
    pending_head: pending_head::PendingHeadInserts,
}

/// Backend for bulk structural 9-byte spender-meta reads on `tx.body`.
///
/// Selected via global `RBITCOIN_IO` (see [`crate::io_backend`]).
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

impl TxTable {
    pub fn create(dir: &Path) -> Result<Self, StoreError> {
        Self::create_with_head_layout(dir, crate::address_head::default_layout())
    }

    /// Create with an explicit head geometry (tests / recovery).
    pub fn create_with_head_layout(dir: &Path, layout: HeadLayout) -> Result<Self, StoreError> {
        let secret = crate::store_secret::StoreSecret::load_or_create(dir, true)?;
        let layout = HeadLayout::with_entry_bytes(layout.bits, 4)?;
        Ok(Self {
            body: VarTable::create(dir, "txout", TableKind::TxOut)?,
            inwit: VarTable::create(dir, "inwit", TableKind::Inwit)?,
            spent: VarTable::create(dir, "spent", TableKind::Spent)?,
            head: SegmentedTxHead::create(dir, layout)?,
            txids: crate::txid_body::TxidBody::create(dir)?,
            secret,
            pending_head: pending_head::PendingHeadInserts::new(),
        })
    }

    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        if dir.join("tx.body").exists() && !dir.join("txout.body").exists() {
            let legacy = VarTable::open(dir, "tx", TableKind::TxOut)?;
            if legacy.count() > 0 {
                return Err(StoreError::Corrupt(
                    "schema 15 refuses packed tx.body with creates; wipe datadir and redo IBD",
                ));
            }
        }
        let had_txout = dir.join("txout.body").exists();
        let had_inwit = dir.join("inwit.body").exists();
        let had_spent = dir.join("spent.body").exists();
        let body = if had_txout {
            VarTable::open(dir, "txout", TableKind::TxOut)?
        } else {
            VarTable::create(dir, "txout", TableKind::TxOut)?
        };
        if had_txout && body.count() > 0 && (!had_inwit || !had_spent) {
            return Err(StoreError::Corrupt(
                "schema 15 Class A missing inwit/spent for existing txout creates; wipe + IBD",
            ));
        }
        let inwit = if had_inwit {
            VarTable::open(dir, "inwit", TableKind::Inwit)?
        } else {
            VarTable::create(dir, "inwit", TableKind::Inwit)?
        };
        let spent = if had_spent {
            VarTable::open(dir, "spent", TableKind::Spent)?
        } else {
            VarTable::create(dir, "spent", TableKind::Spent)?
        };
        let txids = if dir.join("txid.body").exists() {
            crate::txid_body::TxidBody::open(dir)?
        } else {
            crate::txid_body::TxidBody::create(dir)?
        };
        let n_bodies = body.count();
        let n_txids = txids.count();
        let n_inwit = inwit.count();
        let n_spent = spent.count();
        if n_txids != n_bodies || n_inwit != n_bodies || n_spent != n_bodies {
            let n = n_bodies.min(n_txids).min(n_inwit).min(n_spent);
            rbitcoin_log::warn!(
                "store: Class A count skew txout={n_bodies} inwit={n_inwit} spent={n_spent} \
                 txid.body={n_txids} — truncating to {n}"
            );
            if n_bodies > n {
                body.truncate_to_count(n)?;
            }
            if n_inwit > n {
                inwit.truncate_to_count(n)?;
            }
            if n_spent > n {
                spent.truncate_to_count(n)?;
            }
            if n_txids > n {
                txids.truncate_to_count(n)?;
            }
            if body.count() != txids.count()
                || body.count() != inwit.count()
                || body.count() != spent.count()
            {
                return Err(StoreError::Corrupt(
                    "Class A stem counts still mismatch after repair (reindex required)",
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
            inwit,
            spent,
            head,
            txids,
            secret,
            pending_head: pending_head::PendingHeadInserts::new(),
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
            // Crash before write-behind drain: head occupancy lags Class A.
            let n = t.count();
            let covered = t.head.last_inserted_fk();
            if covered < n {
                rbitcoin_log::info!(
                    "store: tx.head lags Class A covered={covered} n={n} — backfill tail"
                );
                t.backfill_head_from(covered.saturating_add(1))?;
                t.head.flush()?;
            }
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

    /// One sequential `tx.body` pread of `[offset, offset+len)`.
    pub fn with_body_span<R>(
        &self,
        offset: u64,
        len: u64,
        f: impl FnOnce(&[u8]) -> Result<R, StoreError>,
    ) -> Result<R, StoreError> {
        self.body.with_bytes_at(offset, len, f)
    }

    /// Contiguous Class A body `(offset, len)` for create_fks `first..=last`.
    pub fn body_ranges(&self, first: u64, last: u64) -> Result<Vec<(u64, u64)>, StoreError> {
        self.body.record_ranges(first, last)
    }

    /// P2TR outs from a packed body slice (stack XOR; no `OutputRecord` heap).
    pub fn packed_p2tr_from_raw(
        &self,
        raw: &[u8],
    ) -> Result<Vec<(u32, [u8; 32], u64)>, StoreError> {
        scan_packed_p2tr_outs(raw, Some(&self.secret))
    }

    /// Meta + input prevouts only (no script/witness allocation, no outputs).
    ///
    /// Used by load: discover parents without full parse into RAM.
    pub fn get_meta_and_prevouts(&self, fk: Fk) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
        let mut tx = self.get(fk)?;
        let inwit = self.inwit.get_raw(fk)?;
        let prevs = scan_inwit_prevouts(&inwit, tx.input_count)?;
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
        let _ = (recs, index);
        Err(StoreError::Corrupt(
            "bare-meta Class A put is refused; use put_full_batch_indexed",
        ))
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

    #[allow(dead_code)]
    /// Relative byte offset of output `vout`'s start inside a `txout` payload.
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

    #[allow(dead_code)]
    /// One packed walk: for each requested `vout`, relative offset of its `txout` start.
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
        if let Some(fk) = self.pending_fk(txid) {
            if self.body_txid(fk)? == *txid {
                crate::head_resolve_stats::add_pending_hit(1);
                crate::head_resolve_stats::add_keys(1);
                crate::head_resolve_stats::add_hit_rank(1);
                return Ok(Some(fk));
            }
        }
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

    /// Sparse outs by known `txout` body ranges (prep pin after plan stamp).
    ///
    /// Each job is `(create_fk, body_range, known_txid, need_vouts)`.
    /// - **Skips `tx.idx`** (range known).
    /// - **`known_txid`**: RAM identity (plan reverse map / residency); not sidefile.
    /// - **`need_vouts`**: sorted unique; empty = all outs. Only those scripts are
    ///   allocated (N2.1). Full body is still pread (layout denserels).
    ///
    /// Returns `(rows, body_ns, decode_ns)` where each row is
    /// `Some((tx, live (vout,out), sparse denserels (vout,rel)))` (N2.0 timers).
    pub fn get_outs_by_range_batch(
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
            BodyMode::Outs,
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

    /// Bulk `body_range` for many fks (confirm load / reconstruct).
    ///
    /// **Sorted** walk of `tx.idx` via [`VarTable::record_range_batch`] (FdOnly
    /// pread segments) —
    /// same modality as archive head-resolve idx (not scatter io_uring/pread).
    pub fn body_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.body.record_range_batch(fks)
    }

    /// `spent.body` range for one create.
    pub fn spent_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.spent.record_range(fk)
    }

    /// `spent.body` ranges (same fk order as [`Self::body_range_batch`]).
    pub fn spent_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.spent.record_range_batch(fks)
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
        let mut in_jobs: Vec<IdxBodyJob> = ranges
            .iter()
            .map(|(fk, _, _)| IdxBodyJob::new(fk.get().unwrap_or(0), None))
            .collect();
        run_idx_body_pipeline(&self.inwit, &mut in_jobs, BodyMode::Full)?;
        let mut out = Vec::with_capacity(jobs.len());
        for ((j, ij), (fk, _, _)) in jobs.into_iter().zip(in_jobs.into_iter()).zip(ranges.iter()) {
            if !j.ok {
                out.push(None);
                continue;
            }
            let mut decoded =
                decode_packed_tx_with_spender_rels_secret(&j.body, Some(&self.secret)).ok();
            if let Some(ref mut d) = decoded {
                if ij.ok {
                    if let Ok(ins) =
                        decode_inwit_secret(&ij.body, d.0.input_count, Some(&self.secret))
                    {
                        d.1 = ins;
                    }
                }
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
        run_idx_body_pipeline(&self.body, &mut jobs, BodyMode::Outs)?;
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
        let body_pub = self.spent.body_published_len();
        let mut cold: Vec<(Fk, u32, Fk)> = Vec::new();
        for &(abs, create_fk, vout, spend_fk) in abs_edges {
            if abs.saturating_add(META_LEN) > body_pub {
                cold.push((create_fk, vout, spend_fk));
                continue;
            }
            let cur = self.spent.with_bytes_at(abs, META_LEN, |raw| {
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
            if let Err(_) = self.spent.write_body_abs(abs, &meta) {
                cold.push((create_fk, vout, spend_fk));
            }
        }
        Ok(cold)
    }

    /// Bulk 9-byte spender meta reads at absolute `tx.body` file offsets.
    ///
    /// Returns `(spender_field, flags)` — multi = `flags & MULTI_SPENDER`.
    /// Backend from [`spend_meta_backend`] / global `RBITCOIN_IO` /
    /// global `RBITCOIN_IO` (`uring` \| `pread`). Out-of-range / short → `None`.
    pub fn get_spender_meta_at_abs_batch(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.get_spender_meta_at_abs_batch_backend(abs_offs, spend_meta_backend())
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
        let body_fd = self.spent.body_read_fd();
        let body_pub = self.spent.body_published_len();
        let body_path = self.spent.body_file_path();

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
                dontcache: false,
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
        let (off, len) = self.spent.record_range(create_tx_fk)?;
        self.get_output_spender_meta_at(off, len, vout)
    }

    /// Like [`Self::get_output_spender_meta`] but uses a cache-held body range (no idx).
    pub fn get_output_spender_meta_at(
        &self,
        body_off: u64,
        body_len: u64,
        vout: u32,
    ) -> Result<(bool, Fk), StoreError> {
        let abs = spent_abs(body_off, vout);
        let end = body_off.saturating_add(body_len);
        if abs.saturating_add(9) > end {
            return Err(StoreError::Corrupt("spent slot OOB"));
        }
        self.spent.with_bytes_at(abs, 9, |raw| {
            if raw.len() < 9 {
                return Err(StoreError::Corrupt("spent meta short"));
            }
            let field = Fk(u64::from_le_bytes(raw[0..8].try_into().unwrap()));
            let multi = raw[8] & output_flags::MULTI_SPENDER != 0;
            Ok((multi, field))
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
        let mut out = Vec::with_capacity(vouts.len());
        for &v in vouts {
            if let Ok((multi, field)) = self.get_output_spender_meta_at(body_off, body_len, v) {
                out.push((v, multi, field));
            }
        }
        Ok(out)
    }

    /// Patch multi + spender_field on create tx output (packed Class A body).
    pub fn set_output_spender_meta(
        &self,
        create_tx_fk: Fk,
        vout: u32,
        multi: bool,
        field: Fk,
    ) -> Result<(), StoreError> {
        let (off, len) = self.spent.record_range(create_tx_fk)?;
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
        let abs = spent_abs(body_off, vout);
        let end = body_off.saturating_add(body_len);
        if abs.saturating_add(9) > end {
            return Err(StoreError::Corrupt("spent slot OOB"));
        }
        let mut slot = [0u8; 9];
        slot[0..8].copy_from_slice(&field.0.to_le_bytes());
        slot[8] = if multi {
            output_flags::MULTI_SPENDER
        } else {
            0
        };
        self.spent.write_body_abs(abs, &slot)?;
        Ok(())
    }

    /// Full tx: `txout` + `inwit` zip.
    pub fn get_full(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        let raw = self.body.get_raw(fk)?;
        let (mut tx, _ins, outs, _) =
            decode_packed_tx_with_spender_rels_secret(&raw, Some(&self.secret))?;
        let inwit = self.inwit.get_raw(fk)?;
        let ins = decode_inwit_secret(&inwit, tx.input_count, Some(&self.secret))?;
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

    /// Append Class A rows: `txout` + `inwit` + zero `spent` + `txid.body`.
    pub fn put_full_batch_indexed(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let est_out: usize = items
            .iter()
            .map(|(_tx, _ins, outs)| {
                16 + TxRecord::BODY_META_LEN + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let est_inwit: usize = items
            .iter()
            .map(|(_tx, ins, _outs)| 16 + ins.iter().map(|i| i.encoded_len()).sum::<usize>())
            .sum();
        let est_spent: usize = items
            .iter()
            .map(|(_tx, _ins, outs)| 16 + outs.len() * OutputRecord::SPENT_SLOT_LEN)
            .sum();
        let base = self.body.count();
        if self.inwit.count() != base || self.spent.count() != base {
            return Err(StoreError::Corrupt("Class A stem count mismatch on append"));
        }
        self.maybe_coupled_roll(items.len() as u64)?;
        let fks = self.append_stems_one_wave(
            items.len(),
            est_out,
            est_inwit,
            est_spent,
            |i, buf| {
                let (tx, ins, outs) = &items[i];
                encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
            },
            |i, buf| encode_inwit_with_secret(&items[i].1, buf, Some(&self.secret)),
            |i, buf| encode_spent_zeros(items[i].2.len() as u32, buf),
        )?;
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
            std::sync::Arc<(TxRecord, Vec<OutputRecord>)>,
            Vec<InputRecord>,
        )],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        if items.is_empty() {
            return Ok(Vec::new());
        }
        let est_out: usize = items
            .iter()
            .map(|(pin, _ins)| {
                let (_tx, outs) = pin.as_ref();
                16 + TxRecord::BODY_META_LEN + outs.iter().map(|o| o.encoded_len()).sum::<usize>()
            })
            .sum();
        let est_inwit: usize = items
            .iter()
            .map(|(_pin, ins)| 16 + ins.iter().map(|i| i.encoded_len()).sum::<usize>())
            .sum();
        let est_spent: usize = items
            .iter()
            .map(|(pin, _ins)| {
                let (_tx, outs) = pin.as_ref();
                16 + outs.len() * OutputRecord::SPENT_SLOT_LEN
            })
            .sum();
        let base = self.body.count();
        if self.inwit.count() != base || self.spent.count() != base {
            return Err(StoreError::Corrupt("Class A stem count mismatch on append"));
        }
        self.maybe_coupled_roll(items.len() as u64)?;
        let fks = self.append_stems_one_wave(
            items.len(),
            est_out,
            est_inwit,
            est_spent,
            |i, buf| {
                let (pin, ins) = &items[i];
                let (tx, outs) = pin.as_ref();
                encode_packed_tx_with_secret(tx, ins, outs, buf, Some(&self.secret));
            },
            |i, buf| encode_inwit_with_secret(&items[i].1, buf, Some(&self.secret)),
            |i, buf| {
                let (_tx, outs) = items[i].0.as_ref();
                encode_spent_zeros(outs.len() as u32, buf);
            },
        )?;
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

    /// Encode and write `txout` + `inwit` + `spent` bodies as one pwrite wave.
    ///
    /// Order is still body → idx → HWM per stem. Not the spend-annotate machine.
    fn append_stems_one_wave(
        &self,
        n: usize,
        est_out: usize,
        est_inwit: usize,
        est_spent: usize,
        encode_out: impl FnMut(usize, &mut Vec<u8>),
        encode_in: impl FnMut(usize, &mut Vec<u8>),
        encode_sp: impl FnMut(usize, &mut Vec<u8>),
    ) -> Result<Vec<Fk>, StoreError> {
        let Some(p_out) = self.body.prepare_batch_encode(n, est_out, encode_out)? else {
            return Ok(Vec::new());
        };
        let Some(p_in) = self.inwit.prepare_batch_encode(n, est_inwit, encode_in)? else {
            return Err(StoreError::Corrupt("Class A inwit prepare empty"));
        };
        let Some(p_sp) = self.spent.prepare_batch_encode(n, est_spent, encode_sp)? else {
            return Err(StoreError::Corrupt("Class A spent prepare empty"));
        };
        crate::var_table::write_prepared_bodies_one_wave(&[
            (&self.body, &p_out),
            (&self.inwit, &p_in),
            (&self.spent, &p_sp),
        ])?;
        let fks = self.body.finish_prepared(p_out)?;
        let fks_in = self.inwit.finish_prepared(p_in)?;
        let fks_sp = self.spent.finish_prepared(p_sp)?;
        if fks != fks_in || fks != fks_sp {
            return Err(StoreError::Corrupt(
                "Class A append fk mismatch across stems",
            ));
        }
        Ok(fks)
    }

    /// Roll all three idx stems together when any body would exceed the soft span.
    fn maybe_coupled_roll(&self, _n_records: u64) -> Result<(), StoreError> {
        // Independent idx append already rolls per-stem on soft span. Coupled
        // first_fk is preserved when we roll all three at the same next fk
        // before the batch if *any* tail would roll on its next start.
        let next_fk = self.body.count().saturating_add(1);
        let next_out = self.body.next_aligned_start();
        let next_in = self.inwit.next_aligned_start();
        let next_sp = self.spent.next_aligned_start();
        let roll = self.body.idx_would_roll(next_fk, next_out)
            || self.inwit.idx_would_roll(next_fk, next_in)
            || self.spent.idx_would_roll(next_fk, next_sp);
        if !roll {
            return Ok(());
        }
        self.body.force_idx_roll(next_fk, next_out)?;
        self.inwit.force_idx_roll(next_fk, next_in)?;
        self.spent.force_idx_roll(next_fk, next_sp)?;
        Ok(())
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
        if let Some(fk) = self.pending_fk(txid) {
            if self.body_txid(fk)? == *txid {
                out.push((fk, self.get(fk)?));
            }
        }
        let mixed = self.secret.mix_txid(txid);
        // probe_candidates already open-first then sealed newest→oldest, deep-first within.
        let cands = self.head.probe_candidates(&mixed)?;
        for fk in cands {
            if out.iter().any(|(have, _)| have.0 == fk.0) {
                continue;
            }
            if self.body_txid(fk)? != *txid {
                continue;
            }
            out.push((fk, self.get(fk)?));
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
        for &(vout, spend_fk) in edges {
            let (multi, field) = self.get_output_spender_meta_at(spent_off, spent_len, vout)?;
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
            self.set_output_spender_meta_at(spent_off, spent_len, vout, new_multi, new_field)?;
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
        while cur <= n {
            let end = (cur + read_batch - 1).min(n);
            let txids = self.body_txid_range(cur, end)?;
            for (i, txid) in txids.into_iter().enumerate() {
                batch.push((txid, Fk(cur + i as u64)));
                if batch.len() >= write_chunk {
                    inserted += batch.len() as u64;
                    self.head_insert_many(&batch)?;
                    batch.clear();
                }
            }
            cur = end + 1;
        }
        if !batch.is_empty() {
            inserted += batch.len() as u64;
            self.head_insert_many(&batch)?;
        }
        Ok(inserted)
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

    /// Publish txid→fk for resolve before durable `tx.head` drain.
    pub fn head_note_pending(&self, entries: &[([u8; 32], Fk)]) {
        self.pending_head.note(entries);
    }

    /// Resolve a create that is in `txid.body` but not yet in `tx.head`.
    pub fn pending_fk(&self, txid: &[u8; 32]) -> Option<Fk> {
        self.pending_head.get(txid)
    }

    /// Drain the pending insert queue via page-grouped [`Self::head_insert_many`].
    pub fn head_drain_pending(&self) -> Result<u64, StoreError> {
        let batch = self.pending_head.take_queued();
        if batch.is_empty() {
            return Ok(0);
        }
        self.head_insert_many(&batch)?;
        self.pending_head.forget(&batch);
        Ok(batch.len() as u64)
    }

    pub fn pending_head_len(&self) -> usize {
        self.pending_head.len()
    }

    /// Bound write-behind: drain if the queue is at/over [`PENDING_HEAD_CAP`].
    pub fn head_drain_pending_if_full(&self) -> Result<(), StoreError> {
        if self.pending_head.len() >= PENDING_HEAD_CAP {
            self.head_drain_pending()?;
        }
        Ok(())
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
        let mut mixed: Vec<([u8; 32], Fk)> = entries
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
            self.head.insert_many(&mut mixed[i..j], force_roll)?;
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
        let span = |t: &VarTable| -> Result<u64, StoreError> {
            let (off0, _) = t.record_range(Fk(first_fk))?;
            let (off1, len1) = t.record_range(Fk(last_fk))?;
            Ok(off1.saturating_add(len1).saturating_sub(off0))
        };
        // Coupled stems: roll/seal when any of txout / inwit / spent exceeds soft.
        Ok(span(&self.body)?
            .max(span(&self.inwit)?)
            .max(span(&self.spent)?))
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
            fuse8_bytes: self.head.sealed_fuse_resident_bytes(),
            open_keys_bytes: self.head.open_keys_resident_bytes(),
            class_c_l2_bytes: 0,
        }
    }

    /// Flush segmented heads only.
    pub fn flush_head(&self) -> Result<(), StoreError> {
        self.head.flush()
    }

    pub fn flush(&self) -> Result<(), StoreError> {
        self.body.flush()?;
        self.inwit.flush()?;
        self.spent.flush()?;
        self.head.flush()?;
        Ok(())
    }

    pub fn flush_async(&self) -> Result<(), StoreError> {
        self.body.flush_async()?;
        self.inwit.flush_async()?;
        self.spent.flush_async()?;
        self.head.flush_async()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
