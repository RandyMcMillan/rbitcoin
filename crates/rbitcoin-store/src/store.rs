use crate::chain::{ConfirmedTable, HeaderTxsTable, StrongTxTable, TxHeightTable};
use crate::epoch::ArchiveEpoch;
use crate::error::StoreError;
use crate::header_table::{HeaderRecord, HeaderTable};
use crate::point_table::{self, PointRecord};
use crate::scripthash::ScriptHashTable;
use crate::spender_table::SpenderTable;
use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
use rbitcoin_primitives::{Fk, Height, SCHEMA_VERSION, STORE_MAGIC};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// Top-level store handle for a datadir `store/` directory.
pub struct Store {
    path: PathBuf,
    pub headers: HeaderTable,
    pub txs: TxTable,
    /// Multi-spender list nodes only (sole spends live on create outputs).
    pub spenders: SpenderTable,
    pub scripthash: ScriptHashTable,
    pub confirmed: ConfirmedTable,
    pub strong_tx: StrongTxTable,
    /// Class C: tx_fk → create height (maturity; not a UTXO set).
    pub tx_height: TxHeightTable,
    /// Class A: header_fk → tx list (archive before tip confirm).
    /// Confirmed heights resolve txs via `confirmed[h]` → this list.
    pub header_txs: HeaderTxsTable,
    epoch: Mutex<ArchiveEpoch>,
}

impl Store {
    pub fn create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        // SH open-address shards open many FDs; raise soft nofile before create.
        crate::file::ensure_nofile_budget();
        let path = path.into();
        if path.exists() {
            if !path.is_dir() {
                return Err(StoreError::NotDirectory(path));
            }
        } else {
            std::fs::create_dir_all(&path).map_err(|e| StoreError::io(&path, e))?;
        }
        write_meta(&path)?;
        let epoch = ArchiveEpoch::default();
        epoch.store(&path)?;
        Ok(Self {
            headers: HeaderTable::create(&path)?,
            txs: TxTable::create(&path)?,
            spenders: SpenderTable::create(&path)?,
            scripthash: ScriptHashTable::create(&path)?,
            confirmed: ConfirmedTable::create(&path)?,
            strong_tx: StrongTxTable::create(&path)?,
            tx_height: TxHeightTable::create(&path)?,
            header_txs: HeaderTxsTable::create(&path)?,
            epoch: Mutex::new(epoch),
            path,
        })
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        crate::file::ensure_nofile_budget();
        let path = path.into();
        if !path.is_dir() {
            return Err(StoreError::NotDirectory(path));
        }
        check_meta(&path)?;
        let epoch = ArchiveEpoch::load(&path)?;
        // Scripthash table is new in Phase 6 — create if missing (upgrade path).
        let scripthash = if path.join("scripthash.body").exists() {
            ScriptHashTable::open(&path)?
        } else {
            ScriptHashTable::create(&path)?
        };
        // header_txs v2: (first, count) arrays (upgrade path if missing).
        let header_txs = if path.join("header_txs_first.body").exists() {
            HeaderTxsTable::open(&path)?
        } else {
            HeaderTxsTable::create(&path)?
        };
        let tx_height = if path.join("tx_height.body").exists() {
            TxHeightTable::open(&path)?
        } else {
            TxHeightTable::create(&path)?
        };
        Ok(Self {
            headers: HeaderTable::open(&path)?,
            txs: TxTable::open(&path)?,
            spenders: SpenderTable::open(&path)?,
            scripthash,
            confirmed: ConfirmedTable::open(&path)?,
            strong_tx: StrongTxTable::open(&path)?,
            tx_height,
            header_txs,
            epoch: Mutex::new(epoch),
            path,
        })
    }

    pub fn open_or_create(path: impl Into<PathBuf>) -> Result<Self, StoreError> {
        let path = path.into();
        if path.join("meta").exists() {
            Self::open(path)
        } else {
            Self::create(path)
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn tip_height(&self) -> Option<Height> {
        self.confirmed.tip_height()
    }

    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        self.headers.put(rec)
    }

    pub fn get_header(&self, fk: Fk) -> Result<HeaderRecord, StoreError> {
        self.headers.get(fk)
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, StoreError> {
        self.headers.get_by_hash(hash)
    }

    /// Total Class A header rows (confirmed + unconfirmed archive path).
    pub fn header_count(&self) -> u64 {
        self.headers.count()
    }

    /// Headers that currently have a Class A body association.
    pub fn archived_block_count(&self) -> Result<u64, StoreError> {
        self.header_txs.count_bodies()
    }

    /// Flush header + body-association tables only (cheaper than full store flush).
    ///
    /// Used by the IBD archive writer so Class A survives unclean restarts without
    /// fsyncing every mega-batch of txs/ins/outs.
    ///
    pub fn flush_header_archive(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.header_txs.flush()?;
        Ok(())
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, StoreError> {
        self.txs.put(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        self.txs.get(fk)
    }

    /// Full Class A body by fk: **one** `tx.body` read (packed only).
    pub fn get_tx_full(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        self.txs.get_full(fk)
    }

    /// Parent-prevout hot path: meta + outputs only (no input materialization).
    pub fn get_tx_meta_and_outputs(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        self.txs.get_meta_and_outputs(fk)
    }

    /// Prewarm: meta + input prevouts only (no script/output allocation).
    pub fn get_tx_meta_and_prevouts(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<([u8; 32], u32)>), StoreError> {
        self.txs.get_meta_and_prevouts(fk)
    }

    /// `mlock` pages for a Class A body; returns page range for later munlock.
    pub fn mlock_tx_body(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.txs.mlock_body(fk)
    }

    /// Best-effort `munlock` for a prior [`Self::mlock_tx_body`] page range.
    pub fn munlock_tx_body_pages(&self, page_start: u64, page_len: u64) {
        self.txs.munlock_body_pages(page_start, page_len);
    }

    /// Best-effort `munlock` for a prior [`crate::MlockRange`].
    pub fn munlock_range(&self, r: &crate::MlockRange) {
        if r.is_empty() {
            return;
        }
        use crate::MlockTable::*;
        match r.table {
            TxBody => self.txs.munlock_body_pages(r.page_start, r.page_len),
            TxIdx => self.txs.munlock_idx_pages(r.page_start, r.page_len),
            TxHead => self.txs.munlock_head_pages(r.page_start, r.page_len),
            HeaderBody => self.headers.munlock_body_pages(r.page_start, r.page_len),
            HeaderHead => self.headers.munlock_head_pages(r.page_start, r.page_len),
            HeaderTxsFirst => self
                .header_txs
                .munlock_first_pages(r.page_start, r.page_len),
            HeaderTxsCount => self
                .header_txs
                .munlock_count_pages(r.page_start, r.page_len),
            Spenders => self.spenders.munlock_pages(r.page_start, r.page_len),
            StrongTx => self.strong_tx.munlock_pages(r.page_start, r.page_len),
            TxHeight => self.tx_height.munlock_pages(r.page_start, r.page_len),
            Confirmed => self.confirmed.munlock_pages(r.page_start, r.page_len),
        }
    }

    fn push_mlock(
        out: &mut Vec<crate::MlockRange>,
        table: crate::MlockTable,
        r: Result<(u64, u64), StoreError>,
    ) {
        match r {
            Ok((ps, pl)) if pl > 0 => out.push(crate::MlockRange {
                table,
                page_start: ps,
                page_len: pl,
            }),
            Ok(_) => {}
            Err(e) => {
                rbitcoin_log::trace!("store: mlock {table:?} failed: {e}");
            }
        }
    }

    /// Pin Class A **body only** (idx is RAM-cached by prewarm; not mlocked).
    pub fn mlock_tx_body_only(&self, fk: Fk) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(1);
        Self::push_mlock(&mut out, crate::MlockTable::TxBody, self.txs.mlock_body(fk));
        out
    }

    /// Pin Class A body pages for a known absolute range (no idx).
    pub fn mlock_tx_body_at(&self, offset: u64, len: u64) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(1);
        Self::push_mlock(
            &mut out,
            crate::MlockTable::TxBody,
            self.txs.mlock_body_at(offset, len),
        );
        out
    }

    /// Coalesce absolute body `(offset, len)` into page spans and `mlock` each.
    ///
    /// Adjacent / overlapping page ranges merge so sequential Class A bodies in
    /// the same file region cost one syscall instead of one per tx.
    pub fn mlock_tx_body_ranges_coalesced(
        &self,
        ranges: &[(u64, u64)],
    ) -> Vec<crate::MlockRange> {
        if ranges.is_empty() {
            return Vec::new();
        }
        const PAGE: u64 = 4096;
        let mut spans: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for &(off, len) in ranges {
            if len == 0 {
                continue;
            }
            let start = off & !(PAGE - 1);
            let end = off.saturating_add(len).saturating_add(PAGE - 1) & !(PAGE - 1);
            let plen = end.saturating_sub(start);
            if plen > 0 {
                spans.push((start, plen));
            }
        }
        if spans.is_empty() {
            return Vec::new();
        }
        spans.sort_unstable_by_key(|(s, _)| *s);
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(spans.len());
        for (s, l) in spans {
            if let Some((ms, ml)) = merged.last_mut() {
                let mend = ms.saturating_add(*ml);
                if s <= mend {
                    let new_end = s.saturating_add(l).max(mend);
                    *ml = new_end.saturating_sub(*ms);
                    continue;
                }
            }
            merged.push((s, l));
        }
        let mut out = Vec::with_capacity(merged.len());
        for (ps, pl) in merged {
            // mlock_range page-aligns again (idempotent for already-aligned).
            Self::push_mlock(
                &mut out,
                crate::MlockTable::TxBody,
                self.txs.mlock_body_at(ps, pl),
            );
        }
        out
    }

    /// Pin Class A idx+body for `fk` (legacy / tests). Prefer body-only + idx cache.
    pub fn mlock_tx_class_a(&self, fk: Fk) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(2);
        Self::push_mlock(&mut out, crate::MlockTable::TxIdx, self.txs.mlock_idx(fk));
        Self::push_mlock(&mut out, crate::MlockTable::TxBody, self.txs.mlock_body(fk));
        out
    }

    /// Absolute body `(offset, len)` for `fk` (for prewarm idx cache).
    pub fn tx_body_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.txs.body_range(fk)
    }

    /// Full tx decode from a cached body range (no idx read).
    pub fn get_tx_full_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        self.txs.get_full_at(offset, len)
    }

    /// Meta + prevouts from a cached body range (no idx read).
    pub fn get_tx_meta_and_prevouts_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<([u8; 32], u32)>), StoreError> {
        self.txs.get_meta_and_prevouts_at(offset, len)
    }

    /// Meta + outputs only from a cached body range (no parent input materialization).
    pub fn get_tx_meta_and_outputs_at(
        &self,
        offset: u64,
        len: u64,
    ) -> Result<(TxRecord, Vec<OutputRecord>), StoreError> {
        self.txs.get_meta_and_outputs_at(offset, len)
    }

    /// Pin `tx.head` probe chain for `txid`.
    pub fn mlock_tx_head_for(&self, txid: &[u8; 32]) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(1);
        Self::push_mlock(
            &mut out,
            crate::MlockTable::TxHead,
            self.txs.mlock_head_probe(txid),
        );
        out
    }

    /// Pin header head+body for `hash`; returns header fk if found.
    pub fn mlock_header_for_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<(Option<(Fk, HeaderRecord)>, Vec<crate::MlockRange>), StoreError> {
        let (found, raw) = self.headers.mlock_by_hash(hash)?;
        let mut out = Vec::new();
        // mlock_by_hash: first range is head, rest body (if any)
        for (i, (ps, pl)) in raw.into_iter().enumerate() {
            let table = if i == 0 {
                crate::MlockTable::HeaderHead
            } else {
                crate::MlockTable::HeaderBody
            };
            if pl > 0 {
                out.push(crate::MlockRange {
                    table,
                    page_start: ps,
                    page_len: pl,
                });
            }
        }
        Ok((found, out))
    }

    /// Pin header_txs first+count for `header_fk`.
    pub fn mlock_header_txs_for(&self, header_fk: Fk) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(2);
        match self.header_txs.mlock_header(header_fk) {
            Ok(((a0, a1), (b0, b1))) => {
                if a1 > 0 {
                    out.push(crate::MlockRange {
                        table: crate::MlockTable::HeaderTxsFirst,
                        page_start: a0,
                        page_len: a1,
                    });
                }
                if b1 > 0 {
                    out.push(crate::MlockRange {
                        table: crate::MlockTable::HeaderTxsCount,
                        page_start: b0,
                        page_len: b1,
                    });
                }
            }
            Err(e) => rbitcoin_log::trace!("store: mlock header_txs failed: {e}"),
        }
        out
    }

    /// Pin Class C strong bit + height slot for a tx (confirm write + spentness).
    pub fn mlock_class_c_tx(&self, fk: Fk) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(2);
        Self::push_mlock(
            &mut out,
            crate::MlockTable::StrongTx,
            self.strong_tx.mlock_fk(fk),
        );
        Self::push_mlock(
            &mut out,
            crate::MlockTable::TxHeight,
            self.tx_height.mlock_fk(fk),
        );
        out
    }

    /// Pin confirmed[] tip neighborhood for tip advance.
    pub fn mlock_confirmed_height(&self, height: u32) -> Vec<crate::MlockRange> {
        let mut out = Vec::with_capacity(1);
        Self::push_mlock(
            &mut out,
            crate::MlockTable::Confirmed,
            self.confirmed.mlock_height(Height(height)),
        );
        out
    }

    /// Pin spender-list + Class C for durable spentness check on `(create_fk, vout)`.
    pub fn mlock_spend_oracle(&self, create_fk: Fk, vout: u32) -> Vec<crate::MlockRange> {
        let mut out = Vec::new();
        // Body already mlocked separately; still pin spender chain if multi.
        let (multi, field) = match self.txs.get_output_spender_meta(create_fk, vout) {
            Ok(m) => m,
            Err(_) => return out,
        };
        if field.is_null() {
            return out;
        }
        if !multi {
            // Sole spender is a tx fk — pin its Class C bits.
            out.extend(self.mlock_class_c_tx(field));
            return out;
        }
        let mut cur = Some(field);
        let mut guard = 0u32;
        while let Some(fk) = cur {
            if guard > 64 {
                break;
            }
            guard += 1;
            Self::push_mlock(
                &mut out,
                crate::MlockTable::Spenders,
                self.spenders.mlock_record(fk),
            );
            match self.spenders.get(fk) {
                Ok((spend_tx, next)) => {
                    out.extend(self.mlock_class_c_tx(spend_tx));
                    cur = if next.is_null() { None } else { Some(next) };
                }
                Err(_) => break,
            }
        }
        out
    }

    /// Append packed full-tx Class A rows (preferred archive path).
    pub fn put_tx_full_batch_indexed(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        self.txs.put_full_batch_indexed(items, index)
    }

    pub fn get_tx_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, StoreError> {
        self.txs.get_by_txid(txid)
    }

    /// Annotate create outpoint as spent by `spending_tx_fk` (by create Class A fk).
    pub fn put_spend_create(
        &self,
        create_tx_fk: Fk,
        out_index: u32,
        spending_tx_fk: Fk,
    ) -> Result<(), StoreError> {
        point_table::put_spend_on_create(
            &self.txs,
            &self.spenders,
            create_tx_fk,
            out_index,
            spending_tx_fk,
        )
    }

    /// Annotate spend using a prewarmed body range (no `tx.idx` / `tx.head` reads).
    pub fn put_spend_create_at(
        &self,
        create_tx_fk: Fk,
        out_index: u32,
        spending_tx_fk: Fk,
        body_off: u64,
        body_len: u64,
    ) -> Result<(), StoreError> {
        point_table::put_spend_on_create_at(
            &self.txs,
            &self.spenders,
            create_tx_fk,
            out_index,
            spending_tx_fk,
            Some((body_off, body_len)),
        )
    }

    /// Resolve `out_txid` via `tx.head`, then [`Self::put_spend_create`].
    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        _spending_input_index: u32,
    ) -> Result<Fk, StoreError> {
        let create_fk = self
            .txs
            .get_by_txid(out_txid)?
            .map(|(fk, _)| fk)
            .ok_or(StoreError::NotFound)?;
        self.put_spend_create(create_fk, out_index, spending_tx_fk)?;
        Ok(spending_tx_fk)
    }

    /// Bulk annotate by out_txid (resolves each create via `tx.head`).
    /// Tuple: `(out_txid, vout, spending_tx_fk, input_index_ignored)`.
    pub fn put_spend_batch(
        &self,
        edges: &[([u8; 32], u32, Fk, u32)],
    ) -> Result<Vec<Fk>, StoreError> {
        let mut out = Vec::with_capacity(edges.len());
        for &(txid, vout, spend_fk, _) in edges {
            self.put_spend(&txid, vout, spend_fk, 0)?;
            out.push(spend_fk);
        }
        Ok(out)
    }

    /// Confirm hot path: edges already have `create_tx_fk` (+ optional body range).
    ///
    /// Tuple: `(create_tx_fk, vout, spending_tx_fk, Option<(body_off, body_len)>)`.
    /// Sorted by create for locality. **No `tx.head`**. Uses body range when present
    /// so **no `tx.idx`** either (prewarm-cached).
    pub fn put_spend_batch_by_create(
        &self,
        edges: &[(Fk, u32, Fk)],
    ) -> Result<(), StoreError> {
        let mut work: Vec<(Fk, u32, Fk)> = edges.to_vec();
        work.sort_unstable_by_key(|(c, v, _)| (c.0, *v));
        for (create_fk, vout, spend_fk) in work {
            self.put_spend_create(create_fk, vout, spend_fk)?;
        }
        Ok(())
    }

    /// Like [`Self::put_spend_batch_by_create`] with prewarmed body ranges.
    /// Tuple: `(create_tx_fk, vout, spending_tx_fk, body_off, body_len)`.
    ///
    /// Groups by create body and applies all vouts with **one** packed walk per
    /// create (no per-edge full input walk).
    pub fn put_spend_batch_by_create_ranged(
        &self,
        edges: &[(Fk, u32, Fk, u64, u64)],
    ) -> Result<(), StoreError> {
        if edges.is_empty() {
            return Ok(());
        }
        let mut work = edges.to_vec();
        // Group by create_fk + body range, then vout.
        work.sort_unstable_by_key(|(c, v, _, off, _)| (c.0, *off, *v));
        let mut i = 0;
        while i < work.len() {
            let (cfk, _, _, off, len) = work[i];
            if cfk.is_null() {
                return Err(StoreError::InvalidFk);
            }
            let mut j = i + 1;
            while j < work.len()
                && work[j].0 == cfk
                && work[j].3 == off
                && work[j].4 == len
            {
                j += 1;
            }
            let batch: Vec<(u32, Fk)> = work[i..j]
                .iter()
                .map(|(_, v, s, _, _)| (*v, *s))
                .collect();
            self.txs
                .put_spends_on_create_at(&self.spenders, off, len, &batch)?;
            i = j;
        }
        Ok(())
    }

    /// Resolve txid → Class A fk without full body decode (head probe + body txid).
    pub fn get_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        self.txs.get_fk_by_txid(txid)
    }

    /// Batch head resolve (prewarm thin). Prefer primary-slot-sorted `txids`.
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<Fk>)>, StoreError> {
        self.txs.get_fk_by_txid_batch(txids)
    }

    /// Spentness by create fk (no `tx.head`). Body must be mlocked / range-known.
    ///
    /// Sole spender: Class C strong on the spender fk. Multi-list is rare in IBD
    /// (would touch `spenders.body` — not prewarm-mlocked by design).
    pub fn has_confirmed_strong_spender_create(
        &self,
        create_tx_fk: Fk,
        out_index: u32,
        body_range: Option<(u64, u64)>,
    ) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let (multi, field) = match body_range {
            Some((off, len)) => self
                .txs
                .get_output_spender_meta_at(off, len, out_index)?,
            None => self.txs.get_output_spender_meta(create_tx_fk, out_index)?,
        };
        if field.is_null() {
            return Ok(false);
        }
        if !multi {
            return self.is_confirmed_strong_at(field, tip);
        }
        // Multi: walk spenders (cold / rare during IBD).
        let mut found = false;
        point_table::for_each_spender_create(
            &self.txs,
            &self.spenders,
            create_tx_fk,
            out_index,
            |spending_tx_fk| {
                if self.is_confirmed_strong_at(spending_tx_fk, tip)? {
                    found = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        Ok(found)
    }

    /// Unspent subset of `vouts` on one create (wave/prewarm hot path).
    ///
    /// With `body_range`, **one** packed body walk for all vouts (not one walk
    /// per vout). Multi-spender lists fall back to the rare cold path.
    pub fn unspent_create_vouts(
        &self,
        create_tx_fk: Fk,
        vouts: &[u32],
        body_range: Option<(u64, u64)>,
    ) -> Result<Vec<u32>, StoreError> {
        if vouts.is_empty() {
            return Ok(Vec::new());
        }
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let metas: Vec<(u32, bool, Fk)> = match body_range {
            Some((off, len)) => self.txs.get_output_spender_metas_at(off, len, vouts)?,
            None => {
                // No range: still prefer one body pin via per-vout meta (idx once
                // each — rare when prewarm registers ranges).
                let mut out = Vec::with_capacity(vouts.len());
                for &v in vouts {
                    let (multi, field) = self.txs.get_output_spender_meta(create_tx_fk, v)?;
                    out.push((v, multi, field));
                }
                out
            }
        };
        let mut unspent = Vec::with_capacity(metas.len());
        for (v, multi, field) in metas {
            if field.is_null() {
                unspent.push(v);
                continue;
            }
            if !multi {
                if !self.is_confirmed_strong_at(field, tip)? {
                    unspent.push(v);
                }
                continue;
            }
            // Multi-list: rare during IBD.
            if !self.has_confirmed_strong_spender_create(create_tx_fk, v, body_range)? {
                unspent.push(v);
            }
        }
        // Vouts missing from body (corrupt / OOB) are treated as not live.
        Ok(unspent)
    }








    /// Multi-list node count only (sole spends do not allocate body rows).
    pub fn spender_list_count(&self) -> u64 {
        self.spenders.count()
    }









    /// True if `tx_fk` is strong **and** its create height is on the confirmed tip.
    ///
    /// Class C writes set `strong_tx` / `tx_height` before advancing `confirmed[]`
    /// (tip is the commit point). After a hard kill mid-batch, strong bits may sit
    /// above tip; those must not count as best-chain spent (else re-confirm of
    /// tip+1 fails with PrevoutSpent).
    pub fn is_confirmed_strong(&self, tx_fk: Fk) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        self.is_confirmed_strong_at(tx_fk, tip)
    }

    /// Like [`Self::is_confirmed_strong`] with a caller-cached tip (connect hot path).
    #[inline]
    pub fn is_confirmed_strong_at(
        &self,
        tx_fk: Fk,
        tip: Option<u32>,
    ) -> Result<bool, StoreError> {
        if !self.strong_tx.is_strong(tx_fk)? {
            return Ok(false);
        }
        let Some(h) = self.tx_height.get(tx_fk)? else {
            // Strong without height: partial Class C write; not tip-committed.
            return Ok(false);
        };
        match tip {
            Some(tip) if h <= tip => Ok(true),
            _ => Ok(false),
        }
    }

    /// True if any annotated spender for this outpoint is confirmed-strong.
    pub fn has_confirmed_strong_spender(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let Some((create_fk, _)) = self.txs.get_by_txid(out_txid)? else {
            return Ok(false);
        };
        let mut found = false;
        point_table::for_each_spender_create(
            &self.txs,
            &self.spenders,
            create_fk,
            out_index,
            |spending_tx_fk| {
                if self.is_confirmed_strong_at(spending_tx_fk, tip)? {
                    found = true;
                    return Ok(false);
                }
                Ok(true)
            },
        )?;
        Ok(found)
    }

    /// Spenders whose spending transaction is confirmed-strong on the best tip.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let mut out = Vec::new();
        for rec in self.spenders_raw(out_txid, out_index)? {
            if self.is_confirmed_strong_at(rec.spending_tx_fk, tip)? {
                out.push(rec);
            }
        }
        Ok(out)
    }

    /// All annotated spenders (including non-strong / reorg history).
    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, StoreError> {
        let Some((create_fk, _)) = self.txs.get_by_txid(out_txid)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        point_table::for_each_spender_create(
            &self.txs,
            &self.spenders,
            create_fk,
            out_index,
            |spending_tx_fk| {
                out.push(PointRecord {
                    out_txid: *out_txid,
                    out_index,
                    spending_tx_fk,
                    spending_input_index: 0,
                    next: Fk::NULL,
                });
                Ok(true)
            },
        )?;
        Ok(out)
    }

    /// Clear `strong_tx` + `tx_height` for txs whose height is above the confirmed tip.
    ///
    /// Heals partial Class C from a hard kill (`kill -9` / power loss) between
    /// marking spends strong and advancing `confirmed[]`. Point multimap rows are
    /// left in place (archive-shaped); they stay invisible to [`Self::spenders`]
    /// until the spending tx is re-confirmed. Returns the number of txs cleared.
    pub fn repair_class_c_above_tip(&self) -> Result<u64, StoreError> {
        let tip_max = self.confirmed.tip_height().map(|t| t.0);
        // Collect runs first so we do not hold tx_height scan state while clearing.
        let mut runs: Vec<(u64, u64)> = Vec::new(); // (start_id inclusive, end_id exclusive)
        let mut run_start: Option<u64> = None;
        let mut run_end: u64 = 0;
        self.tx_height.for_each_set(|tx_fk, h| {
            let above = match tip_max {
                Some(tip) => h > tip,
                None => true, // no tip: any height is uncommitted Class C
            };
            let id = match tx_fk.get() {
                Some(i) => i,
                None => return Ok(()),
            };
            if !above {
                if let Some(s) = run_start.take() {
                    runs.push((s, run_end));
                }
                return Ok(());
            }
            match run_start {
                Some(_) if id == run_end => {
                    run_end = id + 1;
                }
                Some(s) => {
                    runs.push((s, run_end));
                    run_start = Some(id);
                    run_end = id + 1;
                }
                None => {
                    run_start = Some(id);
                    run_end = id + 1;
                }
            }
            Ok(())
        })?;
        if let Some(s) = run_start {
            runs.push((s, run_end));
        }
        let mut cleared = 0u64;
        for (start, end) in runs {
            if end <= start {
                continue;
            }
            let count = end - start;
            cleared += count;
            if count <= u64::from(u32::MAX) {
                let n = count as u32;
                self.strong_tx.set_unstrong_range(Fk(start), n)?;
                self.tx_height.clear_range(Fk(start), n)?;
            } else {
                for id in start..end {
                    self.strong_tx.set_unstrong(Fk(id))?;
                    self.tx_height.clear(Fk(id))?;
                }
            }
        }
        Ok(cleared)
    }

    /// Full durable flush: `msync(MS_SYNC)` + `fdatasync` every table.
    ///
    /// **Host-hostile on multi‑GiB Class A** — use [`Self::flush_for_shutdown`] for
    /// process exit during IBD.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.txs.flush()?;
        self.spenders.flush()?;
        self.scripthash.flush()?;
        self.confirmed.flush()?;
        self.strong_tx.flush()?;
        self.tx_height.flush()?;
        self.header_txs.flush()?;
        Ok(())
    }

    /// Flush durable index tables (spenders / tx head / scripthash).
    pub fn flush_index_tables(&self) -> Result<(), StoreError> {
        crate::ibd_io_policy::set_defer_durable_flush(false);
        self.spenders.flush()?;
        self.txs.flush()?;
        self.scripthash.flush()?;
        Ok(())
    }

    /// Process-exit flush (IBD / SIGTERM). Target: seconds, not minutes.
    ///
    /// 1. Fsync tip / Class C tables only.
    /// 2. MS_ASYNC Class A bodies.
    pub fn flush_for_shutdown(&self) -> Result<(), StoreError> {
        crate::file::try_set_io_best_effort();
        let t0 = std::time::Instant::now();
        rbitcoin_log::info!("store: shutdown flush — fsync tip tables…");
        self.headers.flush()?;
        self.confirmed.flush()?;
        self.strong_tx.flush()?;
        self.tx_height.flush()?;
        self.header_txs.flush()?;
        rbitcoin_log::info!(
            "store: shutdown flush — async Class A… elapsed={:?}",
            t0.elapsed()
        );
        self.txs.flush_async()?;
        self.spenders.flush_async()?;
        self.scripthash.flush_async()?;
        rbitcoin_log::info!(
            "store: shutdown flush done elapsed={:?}",
            t0.elapsed()
        );
        Ok(())
    }

    pub fn epoch(&self) -> ArchiveEpoch {
        self.epoch.lock().unwrap().clone()
    }

    pub fn set_archive_mode(&self, enabled: bool) -> Result<(), StoreError> {
        let mut ep = self.epoch.lock().unwrap();
        ep.archive_mode = enabled;
        ep.store(&self.path)
    }

    /// Finalize (seal) archive through `height` (inclusive). Soft zone is above this height.
    ///
    /// Flushes all tables and persists the epoch. Caller should drop wire entries ≤ height.
    pub fn finalize_through(&self, height: u32) -> Result<(), StoreError> {
        self.flush()?;
        // Best-effort fsync of store directory files is via table flush; epoch syncs itself.
        let mut ep = self.epoch.lock().unwrap();
        ep.archive_mode = true;
        ep.finalized_height = Some(height);
        ep.store(&self.path)
    }
}

fn write_meta(dir: &Path) -> Result<(), StoreError> {
    let path = dir.join("meta");
    let mut f = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|e| StoreError::io(&path, e))?;
    f.write_all(&STORE_MAGIC)
        .map_err(|e| StoreError::io(&path, e))?;
    f.write_all(&SCHEMA_VERSION.to_le_bytes())
        .map_err(|e| StoreError::io(&path, e))?;
    f.flush().map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

fn check_meta(dir: &Path) -> Result<(), StoreError> {
    let path = dir.join("meta");
    let bytes = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    if bytes.len() < 6 {
        return Err(StoreError::Corrupt("meta too short"));
    }
    if bytes[0..4] != STORE_MAGIC {
        return Err(StoreError::BadMagic);
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver != SCHEMA_VERSION {
        return Err(StoreError::BadSchema(ver));
    }
    Ok(())
}
