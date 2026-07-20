use crate::chain::{ConfirmedTable, HeaderTxsTable, StrongTxTable, TxHeightTable};
use crate::epoch::ArchiveEpoch;
use crate::error::StoreError;
use crate::header_table::{HeaderRecord, HeaderTable};
use crate::point_table::PointTable;
use crate::scripthash::ScriptHashTable;
use crate::tx_table::{InputRecord, InputTable, OutputRecord, OutputTable, TxRecord, TxTable};
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
    pub inputs: InputTable,
    pub outputs: OutputTable,
    pub points: PointTable,
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
        // 256-way heads open many FDs; raise soft nofile before create.
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
            inputs: InputTable::create(&path)?,
            outputs: OutputTable::create(&path)?,
            points: PointTable::create(&path)?,
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
            inputs: InputTable::open(&path)?,
            outputs: OutputTable::open(&path)?,
            points: PointTable::open(&path)?,
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
    /// Also **budget-spills** a few chunks of `point.head` / `tx.head` write-behind
    /// (not a full multi‑M dump) so head links advance without multi-minute storms.
    /// Remaining overlay drains via archive interleave + background worker.
    pub fn flush_header_archive(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.header_txs.flush()?;
        let chunk = crate::sharded_hashhead::spill_chunk_size();
        // Cap work: up to 8 chunks each (~800k keys max) with yields between.
        for _ in 0..8 {
            let a = self.points.spill_head_budget(chunk)?;
            let b = self.txs.spill_head_budget(chunk)?;
            if a + b == 0 {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, StoreError> {
        self.txs.put(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        self.txs.get(fk)
    }

    /// Full Class A body by fk: **one** `tx.body` read when packed; legacy
    /// rows fall back to split input/output tables.
    pub fn get_tx_full(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>), StoreError> {
        self.txs.get_full(fk, &self.inputs, &self.outputs)
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

    /// Append one input run (all inputs of a tx). Returns run FK.
    pub fn put_input_run(&self, recs: &[InputRecord]) -> Result<Fk, StoreError> {
        self.inputs.put_run(recs)
    }

    /// Input at local index within a run (`tx.input_start_fk`, `tx.input_count`).
    pub fn get_input_at(&self, run_fk: Fk, count: u32, index: u32) -> Result<InputRecord, StoreError> {
        self.inputs.get_at(run_fk, count, index)
    }

    pub fn get_input_run(&self, run_fk: Fk, count: u32) -> Result<Vec<InputRecord>, StoreError> {
        self.inputs.get_run(run_fk, count)
    }

    /// Append one output run (all outputs of a tx). Returns run FK.
    pub fn put_output_run(&self, recs: &[OutputRecord]) -> Result<Fk, StoreError> {
        self.outputs.put_run(recs)
    }

    pub fn get_output_at(
        &self,
        run_fk: Fk,
        count: u32,
        index: u32,
    ) -> Result<OutputRecord, StoreError> {
        self.outputs.get_at(run_fk, count, index)
    }

    pub fn get_output_run(&self, run_fk: Fk, count: u32) -> Result<Vec<OutputRecord>, StoreError> {
        self.outputs.get_run(run_fk, count)
    }

    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, StoreError> {
        self.points
            .put_spend(out_txid, out_index, spending_tx_fk, spending_input_index)
    }

    /// Bulk point append (see [`crate::point_table::PointTable::put_spend_batch`]).
    pub fn put_spend_batch(
        &self,
        edges: &[([u8; 32], u32, Fk, u32)],
    ) -> Result<Vec<Fk>, StoreError> {
        self.points.put_spend_batch(edges)
    }

    /// Buffer `point.head` upserts in RAM (IBD); spill at cap / flush.
    pub fn enable_point_head_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        self.points.enable_head_write_behind(max_entries)
    }

    pub fn disable_point_head_write_behind(&self) -> Result<(), StoreError> {
        self.points.disable_head_write_behind()
    }

    pub fn spill_point_head(&self) -> Result<(), StoreError> {
        self.points.spill_head()
    }

    pub fn spill_point_head_fast(&self) -> Result<(), StoreError> {
        self.points.spill_head_fast()
    }

    pub fn spill_point_head_budget(&self, max_entries: usize) -> Result<usize, StoreError> {
        self.points.spill_head_budget(max_entries)
    }

    pub fn spill_point_head_step_if_needed(&self) -> Result<usize, StoreError> {
        self.points.spill_head_step_if_needed()
    }

    /// Defer soft-cap `point.head` spills while confirm is live (connect affinity).
    /// Clearing defer does not bulk-spill.
    pub fn set_point_head_defer_spill(&self, defer: bool) -> Result<(), StoreError> {
        self.points.set_head_defer_spill(defer)
    }

    /// Buffer `tx.head` upserts in RAM (optional IBD); spill at cap / flush.
    pub fn enable_tx_head_write_behind(&self, max_entries: usize) -> Result<(), StoreError> {
        self.txs.enable_head_write_behind(max_entries)
    }

    pub fn disable_tx_head_write_behind(&self) -> Result<(), StoreError> {
        self.txs.disable_head_write_behind()
    }

    pub fn spill_tx_head(&self) -> Result<(), StoreError> {
        self.txs.spill_head()
    }

    pub fn spill_tx_head_fast(&self) -> Result<(), StoreError> {
        self.txs.spill_head_fast()
    }

    pub fn spill_tx_head_budget(&self, max_entries: usize) -> Result<usize, StoreError> {
        self.txs.spill_head_budget(max_entries)
    }

    pub fn spill_tx_head_step_if_needed(&self) -> Result<usize, StoreError> {
        self.txs.spill_head_step_if_needed()
    }

    /// Defer soft-cap `tx.head` spills while confirm is live (archive fight).
    /// Clearing defer does not bulk-spill.
    pub fn set_tx_head_defer_spill(&self, defer: bool) -> Result<(), StoreError> {
        self.txs.set_head_defer_spill(defer)
    }

    /// One short-slice step on both head overlays (background worker / archive).
    pub fn spill_heads_step_if_needed(&self) -> Result<(usize, usize), StoreError> {
        let p = self.spill_point_head_step_if_needed()?;
        let t = self.spill_tx_head_step_if_needed()?;
        Ok((p, t))
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

    /// True if any durable point edge for this outpoint is confirmed-strong.
    ///
    /// Early-exits on the first hit; does **not** allocate a spender `Vec`
    /// (connect double-spend only needs empty / non-empty).
    pub fn has_confirmed_strong_spender(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let key = crate::point_table::PointRecord::outpoint_key(out_txid, out_index);
        self.has_confirmed_strong_spender_key(&key, tip)
    }

    /// Like [`Self::has_confirmed_strong_spender`] with a precomputed outpoint key
    /// and tip snapshot — used for wave_fill batch probes sorted by head key.
    pub fn has_confirmed_strong_spender_key(
        &self,
        outpoint_key: &[u8; 32],
        tip: Option<u32>,
    ) -> Result<bool, StoreError> {
        let mut found = false;
        self.points
            .for_each_spender_key(outpoint_key, |spending_tx_fk, _in_idx| {
                if self.is_confirmed_strong_at(spending_tx_fk, tip)? {
                    found = true;
                    return Ok(false); // stop
                }
                Ok(true)
            })?;
        Ok(found)
    }

    /// Spenders whose spending transaction is confirmed-strong on the best tip.
    ///
    /// Filters with tip-bound strong checks so a kill mid-Class-C cannot poison
    /// double-spend checks on restart.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<crate::point_table::PointRecord>, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let mut out = Vec::new();
        self.points
            .for_each_spender(out_txid, out_index, |spending_tx_fk, spending_input_index| {
                if self.is_confirmed_strong_at(spending_tx_fk, tip)? {
                    out.push(crate::point_table::PointRecord {
                        out_txid: *out_txid,
                        out_index,
                        spending_tx_fk,
                        spending_input_index,
                        next: Fk::NULL,
                    });
                }
                Ok(true)
            })?;
        Ok(out)
    }

    /// All point rows including unconfirmed historical spends (raw multimap).
    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<crate::point_table::PointRecord>, StoreError> {
        self.points.spenders(out_txid, out_index)
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

    /// Full durable flush: spill head overlays, `msync(MS_SYNC)` + `fdatasync` every table.
    ///
    /// **Host-hostile on multi‑GiB Class A** — use [`Self::flush_for_shutdown`] for
    /// process exit during IBD.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.txs.flush()?;
        self.inputs.flush()?;
        self.outputs.flush()?;
        self.points.flush()?;
        self.scripthash.flush()?;
        self.confirmed.flush()?;
        self.strong_tx.flush()?;
        self.tx_height.flush()?;
        self.header_txs.flush()?;
        Ok(())
    }

    /// Process-exit flush (IBD / SIGTERM). Target: seconds, not minutes.
    ///
    /// 1. **Fast** spill of head overlays (single apply each — not 32k chunks).
    /// 2. Fsync tip / Class C tables only.
    /// 3. MS_ASYNC Class A bodies **without** a second head spill.
    ///
    /// Caller must stop the archive writer first so it does not refill overlays
    /// mid-spill (that caused a second ~3 min point spill on the old path).
    pub fn flush_for_shutdown(&self) -> Result<(), StoreError> {
        crate::file::try_set_io_best_effort();
        let t0 = std::time::Instant::now();
        let pp = self.points.head_write_behind_len();
        let tp = self.txs.head_write_behind_len();
        rbitcoin_log::info!(
            "store: shutdown flush — FAST spill heads (point pending≈{pp} tx pending≈{tp})…"
        );
        self.points.spill_head_fast()?;
        self.txs.spill_head_fast()?;
        rbitcoin_log::info!(
            "store: shutdown flush — fsync tip tables… elapsed={:?}",
            t0.elapsed()
        );
        self.headers.flush()?;
        self.confirmed.flush()?;
        self.strong_tx.flush()?;
        self.tx_height.flush()?;
        self.header_txs.flush()?;
        rbitcoin_log::info!(
            "store: shutdown flush — async Class A (no re-spill)… elapsed={:?}",
            t0.elapsed()
        );
        // Bodies only + head files already spilled — never call flush_async on
        // points/txs (that re-spilled the whole overlay again).
        self.txs.flush_async_no_spill()?;
        self.inputs.flush_async()?;
        self.outputs.flush_async()?;
        self.points.flush_async_no_spill()?;
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
