use crate::chain::{ConfirmedTable, HeaderTxsTable, StrongTxTable};
use crate::epoch::ArchiveEpoch;
use crate::error::StoreError;
use crate::header_table::{HeaderRecord, HeaderTable};
use crate::height_fence::HeightFence;
use crate::point_table::{self, PointRecord};
use crate::scripthash::ScriptHashTable;
use crate::spender_table::SpenderTable;
use crate::tx_table::{InputRecord, OutputRecord, TxRecord, TxTable};
use rbitcoin_primitives::{schema_file_openable, Fk, Height, SCHEMA_VERSION, STORE_MAGIC};
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
    /// Class A: header_fk → tx list (archive before tip confirm).
    /// Confirmed heights resolve txs via `confirmed[h]` → this list.
    pub header_txs: HeaderTxsTable,
    /// Resident create-height fence (confirmed[] + header_txs). No `tx_height.body`.
    height_fence: std::sync::RwLock<HeightFence>,
    epoch: Mutex<ArchiveEpoch>,
}

/// How txid → Class A fk picks among rows with the same txid.
///
/// Head probe is newest-first. A later **unconnected** row (rejected block)
/// must not hide an older **connected** instance (height fence hit).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TxidResolveMode {
    /// Connected instance only (fence height Some). Else `None`.
    /// Confirm stamp, structural spends, mempool "confirmed?", annotate.
    TipOnly,
    /// Connected if any, else newest unconnected Class A row (RPC / reconstruct).
    TipThenAny,
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
            header_txs: HeaderTxsTable::create(&path)?,
            height_fence: std::sync::RwLock::new(HeightFence::empty()),
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
        let meta_ver = check_meta(&path)?;
        let epoch = ArchiveEpoch::load(&path)?;
        // Scripthash table is new in Phase 6 — create if missing (upgrade path).
        let scripthash = if path.join("scripthash.body").exists() {
            ScriptHashTable::open(&path)?
        } else {
            ScriptHashTable::create(&path)?
        };
        // Schema 13/14→current: empty Class A + empty SH may rewrite meta. Packed
        // tx.body with creates, or a materialized SH index, is refused.
        if (meta_ver == 13 || meta_ver == 14) && SCHEMA_VERSION >= 15 {
            if scripthash.has_durable_index() {
                return Err(StoreError::Corrupt(
                    "schema 14 store has a materialized scripthash index; wipe store/scripthash* (head, body, ovf, runs, include_hwm, cold_progress) and rematerialize for schema 15",
                ));
            }
            if class_a_has_creates(&path) {
                return Err(StoreError::Corrupt(
                    "schema 16 refuses packed Class A with creates; wipe datadir and redo IBD",
                ));
            }
            rewrite_meta_current(&path)?;
        }
        // header_txs v2: (first, count) arrays (upgrade path if missing).
        let header_txs = if path.join("header_txs_first.body").exists() {
            HeaderTxsTable::open(&path)?
        } else {
            HeaderTxsTable::create(&path)?
        };
        let confirmed = ConfirmedTable::open(&path)?;
        let height_fence = HeightFence::from_confirmed(&confirmed, &header_txs)?;
        // Schema 15 leftover: height is O(blocks) in the fence. Drop the 4 B/tx file.
        let leftover_h = path.join("tx_height.body");
        if leftover_h.exists() {
            eprintln!(
                "store: dropping leftover tx_height.body (schema 16 uses a RAM fence from header_txs)"
            );
            let _ = std::fs::remove_file(&leftover_h);
        }
        if meta_ver == 15 && SCHEMA_VERSION == 16 {
            rewrite_meta_current(&path)?;
        }
        Ok(Self {
            headers: HeaderTable::open(&path)?,
            txs: TxTable::open(&path)?,
            spenders: SpenderTable::open(&path)?,
            scripthash,
            confirmed,
            strong_tx: StrongTxTable::open(&path)?,
            header_txs,
            height_fence: std::sync::RwLock::new(height_fence),
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

    fn fence(&self) -> std::sync::RwLockReadGuard<'_, HeightFence> {
        self.height_fence.read().unwrap_or_else(|e| e.into_inner())
    }

    fn fence_write(&self) -> std::sync::RwLockWriteGuard<'_, HeightFence> {
        self.height_fence.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Connected create height from the RAM fence (`None` = unconnected / hole).
    pub fn tx_height_get(&self, tx_fk: Fk) -> Result<Option<u32>, StoreError> {
        if tx_fk.is_null() {
            return Err(StoreError::InvalidFk);
        }
        Ok(self.fence().height_of(tx_fk))
    }

    /// Rebuild the fence from `confirmed[]` + `header_txs` (open / tests).
    pub fn rebuild_height_fence(&self) -> Result<(), StoreError> {
        let f = HeightFence::from_confirmed(&self.confirmed, &self.header_txs)?;
        *self.fence_write() = f;
        Ok(())
    }

    /// After `confirmed[]` advanced: append this height’s Class A run.
    pub fn height_fence_extend(&self, height: Height, header_fk: Fk) -> Result<(), StoreError> {
        let Some((first, n)) = self.header_txs.get_range(header_fk)? else {
            return Ok(());
        };
        self.fence_write().extend(height.0, first, n);
        Ok(())
    }

    /// After tip shrink: drop the disconnected height’s run.
    pub fn height_fence_pop_tip(&self, height: Height) {
        self.fence_write().pop_height(height.0);
    }

    /// Header write gate: unique by full hash; reject false `prev_fk` edges.
    /// See [`HeaderTable::ensure`].
    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        self.headers.ensure(rec)
    }

    /// Append without uniqueness (offline rebuild only).
    pub fn put_header_raw(&self, rec: &HeaderRecord) -> Result<Fk, StoreError> {
        self.headers.put_raw(rec)
    }

    /// In-place rewrite of one header row (offline repair).
    pub fn rewrite_header(&self, fk: Fk, rec: &HeaderRecord) -> Result<(), StoreError> {
        self.headers.rewrite(fk, rec)
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

    /// Occupied slots in the header hash head (load / sizes observer).
    pub fn header_head_occupied(&self) -> u64 {
        self.headers.head_occupied()
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

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, StoreError> {
        self.txs.get(fk)
    }

    /// Full Class A body by fk: zip `txout` + `inwit`.
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

    /// Load: meta + input prevouts only (no script/output allocation).
    pub fn get_tx_meta_and_prevouts(
        &self,
        fk: Fk,
    ) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
        self.txs.get_meta_and_prevouts(fk)
    }

    /// Absolute body `(offset, len)` for `fk` (for cache idx cache).
    pub fn tx_body_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.txs.body_range(fk)
    }

    /// Absolute `spent.body` `(offset, len)` for `fk` (annotate / unspent).
    pub fn tx_spent_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.txs.spent_range(fk)
    }

    /// Absolute `inwit.body` `(offset, len)` for `fk`.
    pub fn tx_inwit_range(&self, fk: Fk) -> Result<(u64, u64), StoreError> {
        self.txs.inwit.record_range(fk)
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
    ) -> Result<(TxRecord, Vec<(Fk, u32)>), StoreError> {
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

    /// Append packed full-tx Class A rows (preferred archive path).
    pub fn put_tx_full_batch_indexed(
        &self,
        items: &[(TxRecord, Vec<InputRecord>, Vec<OutputRecord>)],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        self.txs.put_full_batch_indexed(items, index)
    }

    /// Append Class A rows from shared pin Arc + inputs (no outs reclone).
    ///
    /// `pin` is `(TxRecord, outs)`.
    pub fn put_tx_full_batch_from_pins(
        &self,
        items: &[(
            std::sync::Arc<(TxRecord, Vec<OutputRecord>)>,
            Vec<InputRecord>,
        )],
        index: bool,
    ) -> Result<Vec<Fk>, StoreError> {
        self.txs.put_full_batch_from_pins(items, index)
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

    /// Annotate spend using a cache-held body range (no `tx.idx` / `tx.head` reads).
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
    /// so **no `tx.idx`** either (load-cached).
    pub fn put_spend_batch_by_create(&self, edges: &[(Fk, u32, Fk)]) -> Result<(), StoreError> {
        let mut work: Vec<(Fk, u32, Fk)> = edges.to_vec();
        work.sort_unstable_by_key(|(c, v, _)| (c.0, *v));
        for (create_fk, vout, spend_fk) in work {
            self.put_spend_create(create_fk, vout, spend_fk)?;
        }
        Ok(())
    }

    /// Bulk create heights from the RAM fence (confirm write / BIP68).
    pub fn tx_height_get_batch(&self, fks: &[Fk]) -> Result<Vec<Option<u32>>, StoreError> {
        Ok(self.fence().get_batch(fks))
    }

    /// Among Class A rows for `txid`, prefer a connected fk (fence height Some).
    pub fn resolve_txid(
        &self,
        txid: &[u8; 32],
        mode: TxidResolveMode,
    ) -> Result<Option<Fk>, StoreError> {
        let all = self.txs.get_all_by_txid(txid)?;
        if all.is_empty() {
            return Ok(None);
        }
        let fks: Vec<Fk> = all.iter().map(|(fk, _)| *fk).collect();
        let heights = self.tx_height_get_batch(&fks)?;
        for (fk, h) in fks.iter().zip(heights.iter()) {
            if h.is_some() {
                return Ok(Some(*fk));
            }
        }
        match mode {
            TxidResolveMode::TipOnly => Ok(None),
            TxidResolveMode::TipThenAny => Ok(Some(all[0].0)),
        }
    }

    /// Coinbase Class A fk for each confirmed height (or `None` if tip/header missing).
    ///
    /// Uses only Class C dense tables (`confirmed` + `header_txs_first`) — **no**
    /// `tx.body`. Used by confirm write `create_h` to detect coinbase without
    /// decoding create inputs.
    pub fn coinbase_fk_at_heights(&self, heights: &[u32]) -> Result<crate::U32Map<Fk>, StoreError> {
        use rbitcoin_primitives::Height;
        if heights.is_empty() {
            return Ok(crate::U32Map::default());
        }
        let mut uniq: Vec<u32> = heights.to_vec();
        uniq.sort_unstable();
        uniq.dedup();
        let hs: Vec<Height> = uniq.iter().map(|&h| Height(h)).collect();
        let headers = self.confirmed.get_many(&hs)?;
        let mut out = crate::U32Map::with_capacity_and_hasher(uniq.len(), Default::default());
        for (i, &h) in uniq.iter().enumerate() {
            let Some(hfk) = headers[i] else {
                continue;
            };
            if let Some((first, _)) = self.header_txs.get_range(hfk)? {
                out.insert(h, first);
            }
        }
        Ok(out)
    }

    /// Annotate spends using absolute 9-byte spender-meta offsets (pin layout).
    ///
    /// Tuple: `(abs_off, create_tx_fk, vout, spending_tx_fk)`.
    /// Prefer io_uring RMW (read → sole/multi/promote → write); multi-list nodes
    /// go to `spenders.body` inline on read completion. Returns edges that still
    /// need a full cold path (OOB abs).
    pub fn put_spend_batch_by_abs_meta(
        &self,
        abs_edges: &[(u64, Fk, u32, Fk)],
    ) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
        self.txs
            .put_spend_batch_by_abs_meta(&self.spenders, abs_edges)
    }

    /// Like [`Self::put_spend_batch_by_create`] with cache-held body ranges.
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
            while j < work.len() && work[j].0 == cfk && work[j].3 == off && work[j].4 == len {
                j += 1;
            }
            let batch: Vec<(u32, Fk)> = work[i..j].iter().map(|(_, v, s, _, _)| (*v, *s)).collect();
            self.txs
                .put_spends_on_create_at(&self.spenders, off, len, &batch)?;
            i = j;
        }
        Ok(())
    }

    /// Resolve txid → Class A fk without full body decode (head probe + body txid).
    ///
    /// **`TipThenAny`:** RPC / reconstruct (connected if present, else newest).
    pub fn get_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        self.resolve_txid(txid, TxidResolveMode::TipThenAny)
    }

    /// Confirm / consensus: connected instance only.
    pub fn get_fk_by_txid_tip(&self, txid: &[u8; 32]) -> Result<Option<Fk>, StoreError> {
        self.resolve_txid(txid, TxidResolveMode::TipOnly)
    }

    /// Batch head resolve for plan stamp: txid → (fk, body_range).
    ///
    /// Confirm uses **`TipOnly`**: unconnected first-hits are dropped; a connected
    /// sibling is recovered via [`Self::resolve_txid`] (covers cold-segment
    /// instances the hot wave would miss after an unconnected hit).
    pub fn get_fk_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
        self.get_fk_by_txid_batch_mode(txids, TxidResolveMode::TipOnly)
    }

    /// Batch resolve with explicit mode (RPC may use [`TxidResolveMode::TipThenAny`]).
    ///
    /// Uses the same hot/cold probe machine as [`TxTable::get_fk_by_txid_batch`]:
    /// an unconnected hot hit does **not** skip the cold wave, and every
    /// body_txid match in a wave is considered so a connected sibling wins.
    pub fn get_fk_by_txid_batch_mode(
        &self,
        txids: &[[u8; 32]],
        mode: TxidResolveMode,
    ) -> Result<Vec<([u8; 32], Option<(Fk, (u64, u64))>)>, StoreError> {
        let fence = self.fence();
        crate::head_resolve_denserels::resolve_fk_and_range_batch_with_tip(
            &self.txs,
            &fence,
            txids,
            matches!(mode, TxidResolveMode::TipOnly),
        )
    }

    /// Sparse outs by known `txout` ranges (prep; skips idx).
    ///
    /// See [`TxTable::get_outs_by_range_batch`].
    pub fn get_outs_by_range_batch(
        &self,
        items: &[(Fk, (u64, u64), [u8; 32], Vec<u32>)],
    ) -> Result<
        (
            Vec<Option<(TxRecord, Vec<(u32, OutputRecord)>, Vec<(u32, u32)>)>>,
            u64,
            u64,
        ),
        StoreError,
    > {
        self.txs.get_outs_by_range_batch(items)
    }

    /// Head-resolve: `txid.body` identity + `txout` outs (not Prefix33 body peeks).
    ///
    /// See [`TxTable::get_fk_and_outs_by_txid_batch`].
    pub fn get_fk_and_outs_by_txid_batch(
        &self,
        txids: &[[u8; 32]],
    ) -> Result<
        (
            Vec<(
                [u8; 32],
                Option<(Fk, Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>)>,
            )>,
            u64,
        ),
        StoreError,
    > {
        self.txs.get_fk_and_outs_by_txid_batch(txids)
    }

    /// Bulk Class A body ranges (confirm load / reconstruct).
    ///
    /// Sorted walk of `tx.idx` (FdOnly pread; contiguous runs coalesced). Prefer
    /// [`Self::idx_body_pipeline`] when the caller also needs body bytes.
    pub fn tx_body_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.txs.body_range_batch(fks)
    }

    pub fn tx_spent_range_batch(&self, fks: &[Fk]) -> Result<Vec<Option<(u64, u64)>>, StoreError> {
        self.txs.spent_range_batch(fks)
    }

    /// Completion-driven idx→body io_uring pipeline (confirm load / prep).
    ///
    /// Jobs with pre-known `range` skip idx. See [`crate::run_idx_body_pipeline`].
    pub fn idx_body_pipeline(
        &self,
        jobs: &mut [crate::IdxBodyJob],
        mode: crate::IdxBodyMode,
    ) -> Result<(), StoreError> {
        crate::run_idx_body_pipeline(&self.txs.body, jobs, mode)
    }

    pub fn idx_inwit_pipeline(
        &self,
        jobs: &mut [crate::IdxBodyJob],
        mode: crate::IdxBodyMode,
    ) -> Result<(), StoreError> {
        crate::run_idx_body_pipeline(&self.txs.inwit, jobs, mode)
    }

    /// Bulk full packed decode from known ranges (confirm load).
    ///
    /// Fourth field: dense spender_rels (rel to body_off) for pin/residency layout.
    pub fn get_tx_full_batch_at(
        &self,
        ranges: &[(Fk, u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<InputRecord>, Vec<OutputRecord>, Vec<u32>)>>, StoreError>
    {
        self.txs.get_full_batch_at(ranges)
    }

    /// Bulk meta+outputs+spender_rels from known ranges (confirm pin_new).
    ///
    /// Outs are content-only (spender fields cleared). `spender_rels[v]` is the
    /// relative offset of the 9-byte annotation within the packed body.
    pub fn get_tx_meta_and_outputs_batch_at(
        &self,
        ranges: &[(u64, u64)],
    ) -> Result<Vec<Option<(TxRecord, Vec<OutputRecord>, Vec<u32>)>>, StoreError> {
        self.txs.get_meta_and_outputs_batch_at(ranges)
    }

    /// Bulk 9-byte spender meta at absolute `tx.body` offsets.
    ///
    /// Backend from global `RBITCOIN_IO` (see [`crate::spend_meta_backend`]).
    pub fn get_spender_meta_at_abs_batch(
        &self,
        abs_offs: &[u64],
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.txs.get_spender_meta_at_abs_batch(abs_offs)
    }

    /// Explicit-backend bulk meta (tests / timed structural path).
    pub fn get_spender_meta_at_abs_batch_backend(
        &self,
        abs_offs: &[u64],
        backend: crate::SpendMetaBackend,
    ) -> Result<Vec<Option<(Fk, u8)>>, StoreError> {
        self.txs
            .get_spender_meta_at_abs_batch_backend(abs_offs, backend)
    }

    /// Pure-write annotate with structural-known meta (no body pread).
    ///
    /// `abs_edges`: `(abs_off, create_tx_fk, vout, spending_tx_fk)`.
    /// `known`: parallel `(field, flags)` from structural spentness.
    pub fn put_spend_batch_by_abs_meta_known(
        &self,
        abs_edges: &[(u64, Fk, u32, Fk)],
        known: &[(Fk, u8)],
        backend: crate::spend_annotate_uring::SpendAnnBackend,
    ) -> Result<Vec<(Fk, u32, Fk)>, StoreError> {
        self.txs
            .put_spend_batch_by_abs_meta_known(&self.spenders, abs_edges, known, backend)
    }

    /// Spentness by create fk (no `tx.head`). Prefer known body range when available.
    ///
    /// Sole spender: Class C strong on the spender fk. Multi-list is rare in IBD
    /// (would touch `spenders.body`).
    pub fn has_confirmed_strong_spender_create(
        &self,
        create_tx_fk: Fk,
        out_index: u32,
        body_range: Option<(u64, u64)>,
    ) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        let (multi, field) = match body_range {
            Some((off, len)) => self.txs.get_output_spender_meta_at(off, len, out_index)?,
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

    /// Unspent subset of `vouts` on one create (wave/cache hot path).
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
            // `body_range` here is the create's **spent.body** span (schema 15).
            Some((off, len)) => self.txs.get_output_spender_metas_at(off, len, vouts)?,
            None => {
                if let Ok((off, len)) = self.txs.spent.record_range(create_tx_fk) {
                    self.txs.get_output_spender_metas_at(off, len, vouts)?
                } else {
                    let mut out = Vec::with_capacity(vouts.len());
                    for &v in vouts {
                        let (multi, field) = self.txs.get_output_spender_meta(create_tx_fk, v)?;
                        out.push((v, multi, field));
                    }
                    out
                }
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

    /// True if `tx_fk` is strong **and** sits on the confirmed tip chain.
    ///
    /// Class C writes set `strong_tx` before advancing `confirmed[]` (tip is the
    /// commit point). The height fence is rebuilt/extended only from confirmed
    /// header_txs, so leftover strong bits above tip have no fence height and
    /// do not count as best-chain spent.
    pub fn is_confirmed_strong(&self, tx_fk: Fk) -> Result<bool, StoreError> {
        let tip = self.confirmed.tip_height().map(|t| t.0);
        self.is_confirmed_strong_at(tx_fk, tip)
    }

    /// Like [`Self::is_confirmed_strong`] with a caller-cached tip (connect hot path).
    #[inline]
    pub fn is_confirmed_strong_at(&self, tx_fk: Fk, tip: Option<u32>) -> Result<bool, StoreError> {
        if !self.strong_tx.is_strong(tx_fk)? {
            return Ok(false);
        }
        let Some(h) = self.fence().height_of(tx_fk) else {
            // Strong without a confirmed run: partial Class C write or orphan.
            return Ok(false);
        };
        match tip {
            Some(t) if h <= t => Ok(true),
            _ => Ok(false),
        }
    }

    /// True if `tx_fk` is in the Class A body association for `header_fk`.
    #[inline]
    pub fn header_body_contains(&self, header_fk: Fk, tx_fk: Fk) -> Result<bool, StoreError> {
        self.header_txs.contains_tx(header_fk, tx_fk)
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

    /// Unstrong every fk that is strong but not on the confirmed fence.
    ///
    /// Covers leftover strong above tip (kill mid-confirm) and orphan second
    /// Class A+C copies (not in `confirmed[h]` header_txs). Point rows stay;
    /// they remain invisible to [`Self::spenders`] until re-confirm.
    pub fn repair_class_c_above_tip(&self) -> Result<u64, StoreError> {
        self.repair_strong_not_on_fence()
    }

    /// Same as [`Self::repair_class_c_above_tip`] (fence is the only height oracle).
    pub fn repair_orphan_class_c(&self) -> Result<u64, StoreError> {
        self.repair_strong_not_on_fence()
    }

    fn repair_strong_not_on_fence(&self) -> Result<u64, StoreError> {
        let mut to_clear: Vec<u64> = Vec::new();
        {
            let fence = self.fence();
            self.strong_tx.for_each_strong(|fk| {
                if fence.height_of(fk).is_none() {
                    if let Some(id) = fk.get() {
                        to_clear.push(id);
                    }
                }
                Ok(())
            })?;
        }
        if to_clear.is_empty() {
            return Ok(0);
        }
        to_clear.sort_unstable();
        let mut cleared = 0u64;
        let mut run_start = to_clear[0];
        let mut run_end = to_clear[0] + 1;
        for &id in to_clear.iter().skip(1) {
            if id == run_end {
                run_end = id + 1;
                continue;
            }
            cleared += self.clear_class_c_run(run_start, run_end)?;
            run_start = id;
            run_end = id + 1;
        }
        cleared += self.clear_class_c_run(run_start, run_end)?;
        Ok(cleared)
    }

    fn clear_class_c_run(&self, start: u64, end: u64) -> Result<u64, StoreError> {
        if end <= start {
            return Ok(0);
        }
        let count = end - start;
        if count <= u64::from(u32::MAX) {
            self.strong_tx.set_unstrong_range(Fk(start), count as u32)?;
        } else {
            for id in start..end {
                self.strong_tx.set_unstrong(Fk(id))?;
            }
        }
        Ok(count)
    }

    /// In-RAM Class C L2 images (strong_tx bits + confirmed + header_txs).
    pub fn class_c_l2_resident_bytes(&self) -> u64 {
        self.strong_tx
            .l2_resident_bytes()
            .saturating_add(self.confirmed.l2_resident_bytes())
            .saturating_add(self.header_txs.l2_resident_bytes())
    }

    /// Flush Class C **except** `confirmed[]` (pre-tip half of the barrier).
    ///
    /// Order: `strong_tx` → `header_txs`. Used so a mid-barrier kill can leave
    /// strong durable **above** tip (repairable) without advancing tip. Prefer
    /// [`Self::flush_class_c_tip`] for the full barrier.
    pub fn flush_class_c_pre_tip(&self) -> Result<(), StoreError> {
        // Tip-as-commit: never flush confirmed here.
        // Headers first so conf tip cannot reference a non-durable header_fk.
        self.headers.flush()?;
        self.strong_tx.flush()?;
        self.header_txs.flush()?;
        Ok(())
    }

    /// Full Class C **connect** barrier: pre-tip tables **then** `confirmed[]` last.
    ///
    /// Complete-or-fail per table. Call **before** body-queue dequeue so a kill
    /// mid-commit can re-drive from BQ when the barrier had not finished.
    ///
    /// **Tip last on connect:** if `confirmed` were durable before `strong_tx`,
    /// a mid-barrier kill advances tip with missing strong bits; re-confirm
    /// skips those heights and `repair_class_c_above_tip` only clears leftover
    /// strong not on the fence — permanent unstrong tip txs.
    ///
    /// After confirmed is durable, publish soft [`crate::TIP_SEAL_NAME`] so open
    /// can clamp an incomplete extension that never finished this barrier.
    pub fn flush_class_c_tip(&self) -> Result<(), StoreError> {
        self.flush_class_c_pre_tip()?;
        // Commit point on disk: tip advance only after strong/header_txs.
        self.confirmed.flush()?;
        self.publish_tip_seal()?;
        Ok(())
    }

    /// Flush only `confirmed[]` (tip length / tip header map).
    ///
    /// Used by **disconnect** after RAM truncate so tip shrink is durable **before**
    /// unstrong. Do not use for connect (would tip-first).
    pub fn flush_confirmed_only(&self) -> Result<(), StoreError> {
        self.confirmed.flush()?;
        self.publish_tip_seal()?;
        Ok(())
    }

    /// Class C **disconnect** post-clear barrier: strong after tip already
    /// shrunk and flushed via [`Self::flush_confirmed_only`].
    pub fn flush_class_c_after_disconnect_tip(&self) -> Result<(), StoreError> {
        self.strong_tx.flush()?;
        // header_txs unchanged on disconnect (archive association remains).
        Ok(())
    }

    /// Full durable flush: HWM + `sync_data` every table.
    ///
    /// **Host-hostile on multi‑GiB Class A** — use [`Self::flush_for_shutdown`] for
    /// process exit during IBD.
    pub fn flush(&self) -> Result<(), StoreError> {
        self.headers.flush()?;
        self.txs.flush()?;
        self.spenders.flush()?;
        self.scripthash.flush()?;
        self.flush_class_c_tip()?;
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
        let t0 = std::time::Instant::now();
        rbitcoin_log::info!("store: shutdown flush — fsync tip tables…");
        self.headers.flush()?;
        self.flush_class_c_tip()?;
        rbitcoin_log::info!(
            "store: shutdown flush — async Class A… elapsed={:?}",
            t0.elapsed()
        );
        self.txs.flush_async()?;
        self.spenders.flush_async()?;
        self.scripthash.flush_async()?;
        rbitcoin_log::info!("store: shutdown flush done elapsed={:?}", t0.elapsed());
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

fn class_a_has_creates(dir: &Path) -> bool {
    fn published_len(path: &Path) -> u64 {
        let Ok(mut f) = std::fs::File::open(path) else {
            return 0;
        };
        let mut hdr = [0u8; 16];
        if std::io::Read::read_exact(&mut f, &mut hdr).is_err() {
            return 0;
        }
        u64::from_le_bytes(hdr[8..16].try_into().unwrap_or([0; 8]))
    }
    if published_len(&dir.join("txout.body")) > 16 {
        return true;
    }
    if published_len(&dir.join("txid.body")) > 32 {
        return true;
    }
    if published_len(&dir.join("tx.body")) > 16 {
        return true;
    }
    false
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

/// Overwrite store `meta` with current [`SCHEMA_VERSION`] (silent 13→14 upgrade).
fn rewrite_meta_current(dir: &Path) -> Result<(), StoreError> {
    let path = dir.join("meta");
    let tmp = path.with_extension("meta.tmp");
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.write_all(&STORE_MAGIC)
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.write_all(&SCHEMA_VERSION.to_le_bytes())
            .map_err(|e| StoreError::io(&tmp, e))?;
        f.flush().map_err(|e| StoreError::io(&tmp, e))?;
    }
    std::fs::rename(&tmp, &path).map_err(|e| StoreError::io(&path, e))?;
    Ok(())
}

/// Validate store magic + schema. Returns on-disk version when openable.
fn check_meta(dir: &Path) -> Result<u16, StoreError> {
    let path = dir.join("meta");
    let bytes = std::fs::read(&path).map_err(|e| StoreError::io(&path, e))?;
    if bytes.len() < 6 {
        return Err(StoreError::Corrupt("meta too short"));
    }
    if bytes[0..4] != STORE_MAGIC {
        return Err(StoreError::BadMagic);
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if !schema_file_openable(ver) {
        return Err(StoreError::BadSchema(ver));
    }
    Ok(ver)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx_table::{InputRecord, OutputRecord, TxRecord};

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-store-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&p);
        p
    }

    fn coinbase_item(
        txid: [u8; 32],
        outs: Vec<OutputRecord>,
    ) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
        let n_out = outs.len() as u32;
        (
            TxRecord {
                txid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: n_out,
            },
            vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
            outs,
        )
    }

    /// Class C only: coinbase fk at height is header_txs first — no body.
    #[test]
    fn coinbase_fk_at_heights_matches_first_tx() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let hdr = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x1d00ffff,
            nonce: 1,
            merkle_root: [1u8; 32],
            hash: [2u8; 32],
        };
        let hfk = s.put_header(&hdr).unwrap();
        // Two txs: coinbase + one non-cb (contiguous Class A ids).
        let (cb_tx, cb_in, cb_out) = coinbase_item(
            [10u8; 32],
            vec![OutputRecord {
                value: 50_0000_0000,
                script: vec![0x51],
                spender_field: Fk::NULL,
                multi_spender: false,
            }],
        );
        let cb_fks = s
            .put_tx_full_batch_indexed(&[(cb_tx, cb_in, cb_out)], false)
            .unwrap();
        let non_tx = TxRecord {
            txid: [11u8; 32],
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let non_in = vec![InputRecord {
            prev_txid: [10u8; 32],
            prev_index: 0,
            create_fk: cb_fks[0],
            script_sig: vec![],
            sequence: 0xffff_ffff,
            witness: vec![],
        }];
        let non_out = vec![OutputRecord {
            value: 1,
            script: vec![0x51],
            spender_field: Fk::NULL,
            multi_spender: false,
        }];
        let non_fks = s
            .put_tx_full_batch_indexed(&[(non_tx, non_in, non_out)], false)
            .unwrap();
        let fks = vec![cb_fks[0], non_fks[0]];
        assert_eq!(fks.len(), 2);
        s.header_txs.put_range(hfk, fks[0], 2).unwrap();
        s.confirmed.set(Height(0), hfk).unwrap();
        s.rebuild_height_fence().unwrap();

        let map = s.coinbase_fk_at_heights(&[0, 1, 99]).unwrap();
        assert_eq!(map.get(&0).copied(), Some(fks[0]));
        assert!(!map.contains_key(&1)); // no confirmed height 1
        assert!(!map.contains_key(&99));
        // Non-coinbase is not first.
        assert_ne!(map.get(&0).copied().unwrap(), fks[1]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn store_create_open_archive_spend_and_meta_errors() {
        let dir = tmp();
        // Not a directory when path is a file.
        {
            std::fs::write(&dir, b"x").unwrap();
            assert!(matches!(
                Store::create(&dir),
                Err(StoreError::NotDirectory(_))
            ));
            let _ = std::fs::remove_file(&dir);
        }
        assert!(matches!(
            Store::open(&dir),
            Err(StoreError::NotDirectory(_))
        ));

        let s = Store::create(&dir).unwrap();
        assert_eq!(s.path(), dir.as_path());
        assert!(s.tip_height().is_none());
        assert_eq!(s.header_count(), 0);
        assert_eq!(s.archived_block_count().unwrap(), 0);
        assert_eq!(s.spender_list_count(), 0);
        assert!(!s.epoch().archive_mode);
        s.set_archive_mode(true).unwrap();
        assert!(s.epoch().archive_mode);

        let hdr = HeaderRecord {
            prev_fk: Fk::NULL,
            version: 1,
            timestamp: 1,
            bits: 0x1d00ffff,
            nonce: 1,
            merkle_root: [3u8; 32],
            hash: [4u8; 32],
        };
        let hfk = s.put_header(&hdr).unwrap();
        assert_eq!(s.get_header(hfk).unwrap().hash, [4u8; 32]);
        assert_eq!(s.get_header_by_hash(&[4u8; 32]).unwrap().unwrap().0, hfk);

        let create = coinbase_item(
            [10u8; 32],
            vec![
                OutputRecord::unspent(50, vec![0x51]),
                OutputRecord::unspent(25, vec![0x51]),
            ],
        );
        let fks = s.put_tx_full_batch_indexed(&[create], true).unwrap();
        let create_fk = fks[0];
        let (meta, outs) = s.get_tx_meta_and_outputs(create_fk).unwrap();
        assert_eq!(meta.txid, [10u8; 32]);
        assert_eq!(outs.len(), 2);
        let full = s.get_tx_full(create_fk).unwrap();
        assert_eq!(full.2.len(), 2);
        let (m2, prevs) = s.get_tx_meta_and_prevouts(create_fk).unwrap();
        assert_eq!(m2.txid, [10u8; 32]);
        assert_eq!(prevs.len(), 1);
        let (off, len) = s.tx_body_range(create_fk).unwrap();
        // Body alone has zero txid; identity is sidefile / get_tx_full.
        assert_eq!(s.get_tx_full_at(off, len).unwrap().0.txid, [0u8; 32]);
        assert_eq!(s.get_tx_full(create_fk).unwrap().0.txid, [10u8; 32]);
        assert_eq!(s.get_tx_meta_and_prevouts(create_fk).unwrap().1.len(), 1);
        assert_eq!(s.get_tx_meta_and_outputs_at(off, len).unwrap().1.len(), 2);
        assert_eq!(s.get_fk_by_txid(&[10u8; 32]).unwrap(), Some(create_fk));
        assert_eq!(s.get_tx_by_txid(&[10u8; 32]).unwrap().unwrap().0, create_fk);

        // Second tx spends create vout 0.
        let spend = (
            TxRecord {
                txid: [11u8; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![InputRecord {
                prev_txid: [10u8; 32],
                create_fk,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            vec![OutputRecord::unspent(49, vec![0x51])],
        );
        let spend_fk = s.put_tx_full_batch_indexed(&[spend], true).unwrap()[0];
        s.put_spend_create(create_fk, 0, spend_fk).unwrap();
        // Idempotent re-annotate same sole spender.
        s.put_spend_create(create_fk, 0, spend_fk).unwrap();
        // Multi promote: second spender.
        let spend2 = (
            TxRecord {
                txid: [12u8; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![InputRecord {
                prev_txid: [10u8; 32],
                create_fk,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            vec![OutputRecord::unspent(1, vec![0x51])],
        );
        let spend2_fk = s.put_tx_full_batch_indexed(&[spend2], true).unwrap()[0];
        s.put_spend_create(create_fk, 0, spend2_fk).unwrap();
        assert!(s.spender_list_count() >= 2);

        // Third spender prepends multi list.
        let spend3 = (
            TxRecord {
                txid: [13u8; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![InputRecord {
                prev_txid: [10u8; 32],
                create_fk,
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
            vec![OutputRecord::unspent(1, vec![0x51])],
        );
        let spend3_fk = s.put_tx_full_batch_indexed(&[spend3], true).unwrap()[0];
        s.put_spend(&[10u8; 32], 0, spend3_fk, 0).unwrap();
        s.put_spend_batch(&[([10u8; 32], 1, spend_fk, 0)]).unwrap();
        s.put_spend_batch_by_create(&[(create_fk, 1, spend2_fk)])
            .unwrap();
        let (off, len) = s.tx_body_range(create_fk).unwrap();
        let (soff, slen) = s.tx_spent_range(create_fk).unwrap();
        s.put_spend_create_at(create_fk, 1, spend3_fk, soff, slen)
            .unwrap();
        s.put_spend_batch_by_create_ranged(&[]).unwrap();
        // Re-annotate vout1 with ranged multi path already multi.
        s.put_spend_batch_by_create_ranged(&[(create_fk, 1, spend_fk, soff, slen)])
            .unwrap();

        // Class C: confirm spenders + heights. Body list must include spend_fk
        // (membership is part of is_confirmed_strong).
        s.confirmed.set(Height(0), hfk).unwrap();
        // Contiguous body covering create..spend (sequential put order).
        let body_first = create_fk.0.min(spend_fk.0);
        let body_last = create_fk.0.max(spend_fk.0);
        s.header_txs
            .put_range(hfk, Fk(body_first), (body_last - body_first + 1) as u32)
            .unwrap();
        s.strong_tx.set_strong(spend_fk, hfk).unwrap();
        s.rebuild_height_fence().unwrap();
        assert!(s.is_confirmed_strong(spend_fk).unwrap());
        assert!(!s.is_confirmed_strong(spend2_fk).unwrap());
        assert!(s
            .has_confirmed_strong_spender_create(create_fk, 0, Some((soff, slen)))
            .unwrap());
        assert!(s.has_confirmed_strong_spender(&[10u8; 32], 0).unwrap());
        let unspent = s
            .unspent_create_vouts(create_fk, &[0, 1], Some((soff, slen)))
            .unwrap();
        // vout 0 has confirmed strong spender; vout1 multi without strong may still be unspent
        assert!(!unspent.contains(&0));
        let raw = s.spenders_raw(&[10u8; 32], 0).unwrap();
        assert!(raw.len() >= 2);
        let strong_sp = s.spenders(&[10u8; 32], 0).unwrap();
        assert_eq!(strong_sp.len(), 1);
        assert_eq!(strong_sp[0].spending_tx_fk, spend_fk);

        // Batch helpers
        let ranges = s.tx_body_range_batch(&[create_fk, spend_fk]).unwrap();
        assert_eq!(ranges.len(), 2);
        let full_b = s.get_tx_full_batch_at(&[(create_fk, off, len)]).unwrap();
        assert!(full_b[0].is_some());
        let outs_b = s.get_tx_meta_and_outputs_batch_at(&[(off, len)]).unwrap();
        assert!(outs_b[0].is_some());
        let heights = s.tx_height_get_batch(&[spend_fk, create_fk]).unwrap();
        assert_eq!(heights[0], Some(0));

        assert_eq!(s.archived_block_count().unwrap(), 1);
        s.flush_header_archive().unwrap();
        s.flush_index_tables().unwrap();
        s.flush_for_shutdown().unwrap();
        s.finalize_through(0).unwrap();
        assert_eq!(s.epoch().finalized_height, Some(0));

        // repair: strong not on the fence
        s.strong_tx.set_strong(spend2_fk, hfk).unwrap();
        let cleared = s.repair_class_c_above_tip().unwrap();
        assert!(cleared >= 1);
        assert!(!s.is_confirmed_strong(spend2_fk).unwrap());

        s.flush().unwrap();
        drop(s);

        let s = Store::open(&dir).unwrap();
        assert_eq!(s.header_count(), 1);
        let s2 = Store::open_or_create(&dir).unwrap();
        assert_eq!(s2.header_count(), 1);
        drop(s2);

        // open_or_create on fresh path
        let dir2 = tmp();
        let s3 = Store::open_or_create(&dir2).unwrap();
        assert_eq!(s3.header_count(), 0);
        drop(s3);

        // meta errors
        assert!(matches!(
            check_meta(std::path::Path::new("/no/such")),
            Err(StoreError::Io { .. })
        ));
        {
            let bad = tmp();
            std::fs::create_dir_all(&bad).unwrap();
            std::fs::write(bad.join("meta"), b"xx").unwrap();
            assert!(matches!(check_meta(&bad), Err(StoreError::Corrupt(_))));
            std::fs::write(bad.join("meta"), b"XXXX\x00\x00").unwrap();
            assert!(matches!(check_meta(&bad), Err(StoreError::BadMagic)));
            let mut good_magic = STORE_MAGIC.to_vec();
            good_magic.extend_from_slice(&0u16.to_le_bytes());
            // wrong schema if 0 != SCHEMA_VERSION
            if SCHEMA_VERSION != 0 {
                std::fs::write(bad.join("meta"), &good_magic).unwrap();
                assert!(matches!(check_meta(&bad), Err(StoreError::BadSchema(_))));
            }
            // schema 13 meta alone is openable at the check_meta gate
            let mut v13 = STORE_MAGIC.to_vec();
            v13.extend_from_slice(&13u16.to_le_bytes());
            std::fs::write(bad.join("meta"), &v13).unwrap();
            assert_eq!(check_meta(&bad).unwrap(), 13);
            let _ = std::fs::remove_dir_all(&bad);
        }
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    fn write_store_meta_ver(dir: &Path, ver: u16) {
        let mut bytes = STORE_MAGIC.to_vec();
        bytes.extend_from_slice(&ver.to_le_bytes());
        std::fs::write(dir.join("meta"), bytes).unwrap();
    }

    fn read_store_meta_ver(dir: &Path) -> u16 {
        let bytes = std::fs::read(dir.join("meta")).unwrap();
        u16::from_le_bytes([bytes[4], bytes[5]])
    }

    /// Schema 13 with empty SH is layout-compatible: open succeeds and meta
    /// is rewritten to 14. Also stamps empty SHAL alloc v1 → v2 (real 13 body).
    #[test]
    fn open_schema13_empty_scripthash_upgrades_meta_to_14() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            assert!(!s.scripthash.has_durable_index());
            s.flush().unwrap();
        }
        // Real schema-13 stores have SHAL alloc v1 on scripthash.body.
        {
            use crate::file::{TableFile, FILE_HEADER_LEN};
            use crate::scripthash_layout::{SH_ALLOC_HEADER_LEN, SH_ALLOC_MAGIC};
            use rbitcoin_primitives::TableKind;
            let body_path = dir.join("scripthash.body");
            let body = TableFile::open(&body_path, TableKind::ScriptHash).unwrap();
            let mut hdr = [0u8; 24];
            body.read_at(FILE_HEADER_LEN as u64, &mut hdr).unwrap();
            hdr[4..6].copy_from_slice(&1u16.to_le_bytes());
            let mut page = vec![0u8; SH_ALLOC_HEADER_LEN];
            page[..24].copy_from_slice(&hdr);
            // Preserve freelist zeros already in file for rest of page.
            body.read_at(FILE_HEADER_LEN as u64, &mut page).unwrap();
            page[0..4].copy_from_slice(&SH_ALLOC_MAGIC);
            page[4..6].copy_from_slice(&1u16.to_le_bytes());
            body.write_at(FILE_HEADER_LEN as u64, &page).unwrap();
            body.flush().unwrap();
        }
        write_store_meta_ver(&dir, 13);
        assert_eq!(read_store_meta_ver(&dir), 13);

        let s = Store::open(&dir).unwrap();
        assert!(!s.scripthash.has_durable_index());
        drop(s);
        assert_eq!(read_store_meta_ver(&dir), SCHEMA_VERSION);

        // Re-open stays 14.
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.header_count(), 0);
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_full_and_inwit_prevouts_at() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let item = (
            TxRecord {
                txid: [8u8; 32],
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
            vec![OutputRecord::unspent(1, vec![0x51])],
        );
        let fk = s.put_tx_full_batch_indexed(&[item], true).unwrap()[0];
        let (off, len) = s.tx_body_range(fk).unwrap();
        let (m, prevs) = s.get_tx_meta_and_prevouts_at(off, len).unwrap();
        assert_eq!(m.input_count, 1);
        assert!(prevs.is_empty());
        assert!(s.tx_inwit_range(fk).unwrap().1 > 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_schema14_packed_tx_body_with_creates_refused() {
        use crate::file::{TableFile, FILE_HEADER_LEN};
        use rbitcoin_primitives::TableKind;
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            s.flush().unwrap();
        }
        let path = dir.join("tx.body");
        let f = TableFile::create(&path, TableKind::TxOut).unwrap();
        f.write_at(FILE_HEADER_LEN as u64, &[0xABu8; 16]).unwrap();
        f.flush().unwrap();
        write_store_meta_ver(&dir, 14);
        match Store::open(&dir) {
            Ok(_) => panic!("expected refuse for packed tx.body"),
            Err(StoreError::Corrupt(m)) => {
                assert!(m.contains("packed Class A") || m.contains("tx.body"), "{m}");
            }
            Err(other) => panic!("expected Corrupt, got {other}"),
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Schema 14 with empty SH is Class-A compatible: open succeeds and meta
    /// is rewritten to 15. Page-era SHAL stays empty (no dual-read of pages).
    #[test]
    fn open_schema14_empty_scripthash_upgrades_meta_to_15() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            assert!(!s.scripthash.has_durable_index());
            s.flush().unwrap();
        }
        // Real schema-14 stores have SHAL alloc v2 on scripthash.body.
        {
            use crate::file::{TableFile, FILE_HEADER_LEN};
            use crate::scripthash_layout::{SH_ALLOC_HEADER_LEN, SH_ALLOC_MAGIC};
            use rbitcoin_primitives::TableKind;
            let body_path = dir.join("scripthash.body");
            let body = TableFile::open(&body_path, TableKind::ScriptHash).unwrap();
            let mut page = vec![0u8; SH_ALLOC_HEADER_LEN];
            body.read_at(FILE_HEADER_LEN as u64, &mut page).unwrap();
            page[0..4].copy_from_slice(&SH_ALLOC_MAGIC);
            page[4..6].copy_from_slice(&2u16.to_le_bytes());
            body.write_at(FILE_HEADER_LEN as u64, &page).unwrap();
            body.flush().unwrap();
        }
        write_store_meta_ver(&dir, 14);
        assert_eq!(read_store_meta_ver(&dir), 14);

        let s = Store::open(&dir).unwrap();
        assert!(!s.scripthash.has_durable_index());
        drop(s);
        assert_eq!(SCHEMA_VERSION, 16);
        assert_eq!(read_store_meta_ver(&dir), SCHEMA_VERSION);

        let s = Store::open(&dir).unwrap();
        assert_eq!(s.header_count(), 0);
        drop(s);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Schema 14 with a durable page-era SH index cannot open.
    #[test]
    fn open_schema14_with_materialized_scripthash_refused() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            let sh = [0xcdu8; 32];
            s.scripthash
                .put_create(&crate::scripthash::ScriptHashRecord::from_fk(sh, Fk(1)))
                .unwrap();
            assert!(s.scripthash.has_durable_index());
            s.flush().unwrap();
        }
        write_store_meta_ver(&dir, 14);
        match Store::open(&dir) {
            Ok(_) => panic!("expected refuse for schema 14 with durable SH"),
            Err(StoreError::Corrupt(m)) => {
                assert!(
                    m.contains("wipe store/scripthash") || m.contains("schema 14"),
                    "{m}"
                );
            }
            Err(other) => panic!("expected Corrupt, got {other}"),
        }
        // Meta left at 14 (no silent bump on refuse).
        assert_eq!(read_store_meta_ver(&dir), 14);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Schema 13 with a durable SH head cannot open (slab layout incompatible).
    #[test]
    fn open_schema13_with_materialized_scripthash_refused() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            let sh = [0xabu8; 32];
            s.scripthash
                .put_create(&crate::scripthash::ScriptHashRecord::from_fk(sh, Fk(1)))
                .unwrap();
            assert!(s.scripthash.has_durable_index());
            s.flush().unwrap();
        }
        write_store_meta_ver(&dir, 13);
        match Store::open(&dir) {
            Ok(_) => panic!("expected refuse for schema 13 with durable SH"),
            Err(StoreError::Corrupt(m)) => {
                assert!(
                    m.contains("materialized scripthash") || m.contains("schema 13"),
                    "{m}"
                );
            }
            Err(other) => panic!("expected Corrupt, got {other}"),
        }
        // Meta left at 13 (no silent bump on refuse).
        assert_eq!(read_store_meta_ver(&dir), 13);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Mid-barrier kill: pre-tip flush only must not advance durable tip.
    ///
    /// Simulates kill after strong/height durable but before confirmed flush.
    /// Reopen: tip stays old; strong above tip is repaired; no permanent unstrong tip.
    #[test]
    fn class_c_barrier_pre_tip_only_does_not_advance_tip() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            // Genesis tip: height 0 → header fk 1, one strong tx.
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();
            assert_eq!(s.confirmed.tip_height(), Some(Height(0)));

            // In-RAM tip extension (height 1) + strong for new txs — no full barrier.
            s.strong_tx.set_strong_range(Fk(2), 3, Fk(2)).unwrap();
            s.header_txs.put_range(Fk(2), Fk(2), 3).unwrap();
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            // Mid-barrier: strong/height durable, confirmed still unflushed.
            s.flush_class_c_pre_tip().unwrap();
            // Process still sees tip 1 in RAM.
            assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
            // Drop without flushing confirmed (kill mid-barrier).
        }
        let s = Store::open(&dir).unwrap();
        // Durable tip must remain 0 — confirmed was not in pre_tip flush.
        assert_eq!(
            s.confirmed.tip_height(),
            Some(Height(0)),
            "mid-barrier kill must not leave tip ahead of last full barrier"
        );
        assert!(s.strong_tx.is_strong(Fk(1)).unwrap());
        // New strong may be durable above tip; repair clears them.
        let cleared = s.repair_class_c_above_tip().unwrap();
        assert!(
            cleared >= 1,
            "strong/height above tip should be repairable (got cleared={cleared})"
        );
        assert!(!s.is_confirmed_strong(Fk(2)).unwrap());
        assert!(!s.is_confirmed_strong(Fk(3)).unwrap());
        // Tip tx still strong.
        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Full barrier: tip + strong both durable; reopen matches.
    #[test]
    fn class_c_barrier_full_flush_reopen_tip_with_strong() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();

            s.strong_tx.set_strong_range(Fk(2), 2, Fk(2)).unwrap();
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            s.header_txs.put_range(Fk(2), Fk(2), 2).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
        assert_eq!(s.confirmed.get(Height(1)).unwrap(), Some(Fk(2)));
        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        assert!(s.is_confirmed_strong(Fk(2)).unwrap());
        assert!(s.is_confirmed_strong(Fk(3)).unwrap());
        assert_eq!(s.repair_class_c_above_tip().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write-behind: TipOnly resolve hits the pending map before `tx.head` drain.
    #[test]
    fn get_fk_by_txid_tip_hits_pending_before_head_drain() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        s.confirmed.set(Height(0), Fk(1)).unwrap();
        s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
        s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
        let item = coinbase_item([0x51; 32], vec![OutputRecord::unspent(1, vec![0x51])]);
        let fks = s
            .put_tx_full_batch_indexed(&[item], /*index=*/ false)
            .unwrap();
        s.txs.head_note_pending(&[([0x51; 32], fks[0])]);
        s.rebuild_height_fence().unwrap();
        assert_eq!(s.get_fk_by_txid_tip(&[0x51; 32]).unwrap(), Some(fks[0]));
        assert_eq!(s.txs.head_drain_pending().unwrap(), 1);
        assert_eq!(s.get_fk_by_txid_tip(&[0x51; 32]).unwrap(), Some(fks[0]));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Documented hazard if tip flushed without strong: tip advanced, missing strong.
    ///
    /// Proves why order is strong→height→header_txs→confirmed: after this bad
    /// partial sequence, reopen has tip with unstrong txs that repair cannot fix
    /// (only clears above tip). Production never calls this sequence.
    #[test]
    fn class_c_tip_without_strong_is_unrepairable_hazard() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();

            // New tip height only — intentionally skip strong (hazard).
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            s.confirmed.flush().unwrap(); // tip durable without strong
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
        // No strong for height-1 txs; repair only clears ABOVE tip.
        assert_eq!(s.repair_class_c_above_tip().unwrap(), 0);
        // is_confirmed_strong needs height ≤ tip AND strong — missing strong ⇒ false.
        // There is no durable strong for Fk(2); tip is already 1 — permanent gap.
        assert!(!s.strong_tx.is_strong(Fk(2)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Disconnect mid-barrier after tip shrink only: leftover strong/height above
    /// tip is repairable (not permanent unstrong-at-tip).
    #[test]
    fn class_c_disconnect_tip_first_mid_barrier_is_repairable() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            // Tip 0 + tip 1 fully durable.
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            s.header_txs.put_range(Fk(2), Fk(2), 2).unwrap();
            s.strong_tx.set_strong_range(Fk(2), 2, Fk(2)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();
            assert_eq!(s.confirmed.tip_height(), Some(Height(1)));

            // Disconnect tip-first: shrink tip + flush confirmed only (kill before unstrong).
            s.confirmed.disconnect_tip(Height(1)).unwrap();
            s.flush_confirmed_only().unwrap();
            // Do not unstrong — simulate kill mid-disconnect.
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(
            s.confirmed.tip_height(),
            Some(Height(0)),
            "tip shrink must be durable after flush_confirmed_only"
        );
        // Strong may still mark height-1 txs; they are not on the new fence.
        assert!(s.strong_tx.is_strong(Fk(2)).unwrap());
        let cleared = s.repair_class_c_above_tip().unwrap();
        assert!(
            cleared >= 1,
            "strong/height above new tip must be repairable (cleared={cleared})"
        );
        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        assert!(!s.is_confirmed_strong(Fk(2)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Full disconnect barrier sequence (tip shrink → unstrong/height → flush).
    #[test]
    fn class_c_disconnect_full_sequence_reopen_clean() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            s.header_txs.put_range(Fk(2), Fk(2), 2).unwrap();
            s.strong_tx.set_strong_range(Fk(2), 2, Fk(2)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();

            // Production disconnect order (store half).
            s.confirmed.disconnect_tip(Height(1)).unwrap();
            s.flush_confirmed_only().unwrap();
            s.strong_tx.set_unstrong_range(Fk(2), 2).unwrap();
            s.height_fence_pop_tip(Height(1));
            s.flush_class_c_after_disconnect_tip().unwrap();
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(0)));
        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        assert!(!s.strong_tx.is_strong(Fk(2)).unwrap());
        assert_eq!(s.tx_height_get(Fk(2)).unwrap(), None);
        assert_eq!(s.repair_class_c_above_tip().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Hazard: clear strong/height while tip still high, then kill (old disconnect bug).
    #[test]
    fn class_c_disconnect_unstrong_before_tip_is_unrepairable_hazard() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 1).unwrap();
            s.strong_tx.set_strong(Fk(1), Fk(1)).unwrap();
            s.confirmed.set(Height(1), Fk(2)).unwrap();
            s.header_txs.put_range(Fk(2), Fk(2), 1).unwrap();
            s.strong_tx.set_strong(Fk(2), Fk(2)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.flush_class_c_tip().unwrap();

            // Bad order: unstrong while tip still 1.
            s.strong_tx.set_unstrong(Fk(2)).unwrap();
            s.strong_tx.flush().unwrap();
            // Tip still 1 on disk — kill before confirmed.truncate.
        }
        let s = Store::open(&dir).unwrap();
        assert_eq!(s.confirmed.tip_height(), Some(Height(1)));
        // Tip-high + unstrong / no height: repair only clears ABOVE tip — no help.
        assert_eq!(s.repair_class_c_above_tip().unwrap(), 0);
        assert!(!s.is_confirmed_strong(Fk(2)).unwrap());
        assert!(!s.strong_tx.is_strong(Fk(2)).unwrap());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Orphan Class C at tip height (second body not in confirmed header_txs)
    /// must not count as confirmed-strong, and repair_orphan_class_c clears it.
    #[test]
    fn orphan_class_c_at_tip_height_not_confirmed_strong_and_repairable() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        // Real tip body: txs 1..=2 under header 1.
        s.confirmed.set(Height(0), Fk(1)).unwrap();
        s.header_txs.put_range(Fk(1), Fk(1), 2).unwrap();
        s.strong_tx.set_strong_range(Fk(1), 2, Fk(1)).unwrap();
        s.rebuild_height_fence().unwrap();
        // Orphan second copy: txs 3..=4 strong, not in header_txs.
        s.strong_tx.set_strong_range(Fk(3), 2, Fk(99)).unwrap();
        s.flush_class_c_tip().unwrap();

        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        assert!(s.is_confirmed_strong(Fk(2)).unwrap());
        assert!(
            !s.is_confirmed_strong(Fk(3)).unwrap(),
            "orphan at tip height must not be confirmed-strong"
        );
        assert!(!s.is_confirmed_strong(Fk(4)).unwrap());

        let n = s.repair_orphan_class_c().unwrap();
        assert!(n >= 2, "cleared={n}");
        assert!(!s.strong_tx.is_strong(Fk(3)).unwrap());
        assert_eq!(s.tx_height_get(Fk(3)).unwrap(), None);
        assert!(s.is_confirmed_strong(Fk(1)).unwrap());
        assert_eq!(s.repair_orphan_class_c().unwrap(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No tip → repair is a no-op; gapped orphans clear as separate runs.
    #[test]
    fn repair_orphan_class_c_empty_tip_and_gapped_orphans() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            assert_eq!(s.repair_orphan_class_c().unwrap(), 0);
            // Tip body 1..=2; orphans 5 and 10 (non-adjacent → two clear runs).
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.header_txs.put_range(Fk(1), Fk(1), 2).unwrap();
            s.strong_tx.set_strong_range(Fk(1), 2, Fk(1)).unwrap();
            s.rebuild_height_fence().unwrap();
            s.strong_tx.set_strong(Fk(5), Fk(99)).unwrap();
            s.strong_tx.set_strong(Fk(10), Fk(99)).unwrap();
            s.flush_class_c_tip().unwrap();
            let n = s.repair_orphan_class_c().unwrap();
            assert_eq!(n, 2, "cleared gapped orphans");
            assert!(!s.strong_tx.is_strong(Fk(5)).unwrap());
            assert!(!s.strong_tx.is_strong(Fk(10)).unwrap());
            assert!(s.is_confirmed_strong(Fk(1)).unwrap());
            // Strong not on the fence (same repair as above-tip leftovers).
            s.strong_tx.set_strong(Fk(20), Fk(1)).unwrap();
            assert_eq!(s.repair_class_c_above_tip().unwrap(), 1);
            assert!(!s.strong_tx.is_strong(Fk(20)).unwrap());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Upgrade open paths: missing optional tables recreated; unspent without range.
    #[test]
    fn store_open_upgrade_missing_tables_and_unspent_no_range() {
        let dir = tmp();
        {
            let s = Store::create(&dir).unwrap();
            let create = coinbase_item([20u8; 32], vec![OutputRecord::unspent(10, vec![0x51])]);
            let fk = s.put_tx_full_batch_indexed(&[create], true).unwrap()[0];
            s.flush().unwrap();
            drop(s);
            // Remove optional tables so open recreate branches run.
            let _ = std::fs::remove_file(dir.join("scripthash.body"));
            let _ = std::fs::remove_dir_all(dir.join("scripthash.head"));
            let _ = std::fs::remove_file(dir.join("scripthash.head"));
            let _ = std::fs::remove_file(dir.join("header_txs_first.body"));
            let _ = std::fs::remove_file(dir.join("header_txs_count.body"));
            let _ = std::fs::remove_file(dir.join("tx_height.body"));
            let s = Store::open(&dir).unwrap();
            assert_eq!(s.get_tx(fk).unwrap().txid, [20u8; 32]);
            // unspent without body_range
            let u = s.unspent_create_vouts(fk, &[0], None).unwrap();
            assert_eq!(u, vec![0]);
            // empty vouts
            assert!(s.unspent_create_vouts(fk, &[], None).unwrap().is_empty());
            // has_confirmed without range, no spender
            assert!(!s.has_confirmed_strong_spender_create(fk, 0, None).unwrap());
            assert!(!s.has_confirmed_strong_spender(&[20u8; 32], 0).unwrap());
            assert!(s.spenders_raw(&[20u8; 32], 0).unwrap().is_empty());
            assert!(s.spenders(&[9u8; 32], 0).unwrap().is_empty());
            assert_eq!(s.repair_class_c_above_tip().unwrap(), 0);
            drop(s);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_txid_prefers_connected_over_newer_unconnected() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        let txid = [0xABu8; 32];
        let rec = |lock| TxRecord {
            txid,
            version: 1,
            locktime: lock,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk::NULL,
            output_count: 1,
        };
        let out = vec![OutputRecord::unspent(1, vec![0x51])];
        let old = s
            .put_tx_full_batch_indexed(&[(rec(1), vec![], out.clone())], true)
            .unwrap()[0];
        let new = s
            .put_tx_full_batch_indexed(&[(rec(2), vec![], out)], true)
            .unwrap()[0];
        assert_ne!(old, new);
        assert_eq!(
            s.resolve_txid(&txid, TxidResolveMode::TipThenAny).unwrap(),
            Some(new)
        );
        assert_eq!(
            s.resolve_txid(&txid, TxidResolveMode::TipOnly).unwrap(),
            None
        );
        s.header_txs.put_range(Fk(1), old, 1).unwrap();
        s.confirmed.set(Height(0), Fk(1)).unwrap();
        s.rebuild_height_fence().unwrap();
        assert_eq!(
            s.resolve_txid(&txid, TxidResolveMode::TipOnly).unwrap(),
            Some(old),
            "connected older row must win"
        );
        assert_eq!(
            s.resolve_txid(&txid, TxidResolveMode::TipThenAny).unwrap(),
            Some(old)
        );
        let batch_tip = s
            .get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipOnly)
            .unwrap();
        assert_eq!(batch_tip[0].1.map(|(f, _)| f), Some(old));
        let batch_any = s
            .get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipThenAny)
            .unwrap();
        assert_eq!(batch_any[0].1.map(|(f, _)| f), Some(old));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Connected sibling in a **cold** sealed age must beat a newer unconnected
    /// hot hit (`TipThenAny` and `TipOnly`). Wave 1 is only open+3; skipping
    /// wave 2 after an unconnected hot cand would regress to the newer row.
    #[test]
    fn tip_then_any_connected_in_cold_beats_unconnected_hot() {
        use crate::head_resolve_stats::sealed_age_for_fk;
        use crate::segmented_head::{SegmentedTxHead, HEAD_PROBE_HOT_MAX_AGE};
        use crate::tx_table::OutputRecord;

        fn put_one(s: &Store, txid: [u8; 32], lock: u32) -> Fk {
            let rec = TxRecord {
                txid,
                version: 1,
                locktime: lock,
                input_start_fk: Fk::NULL,
                input_count: 0,
                output_start_fk: Fk::NULL,
                output_count: 1,
            };
            let out = vec![OutputRecord::unspent(1, vec![0x51; 96])];
            s.put_tx_full_batch_indexed(&[(rec, vec![], out)], true)
                .unwrap()[0]
        }

        SegmentedTxHead::test_with_soft_span_bytes(48, || {
            let dir = tmp();
            let s = Store::create(&dir).unwrap();
            let txid = [0xCDu8; 32];
            let old = put_one(&s, txid, 1);
            let mut n = 10u64;
            let mut guard = 0u32;
            loop {
                assert_eq!(
                    SegmentedTxHead::soft_span_bytes(),
                    48,
                    "this thread's 48-byte roll window must not be stolen"
                );
                let first = s.txs.head.first_fks_snapshot();
                let age = sealed_age_for_fk(&first, old.0).unwrap_or(0);
                if age > HEAD_PROBE_HOT_MAX_AGE && s.txs.head.sealed_segment_count() >= 4 {
                    break;
                }
                let mut dummy = [0u8; 32];
                dummy[0..8].copy_from_slice(&n.to_le_bytes());
                dummy[15] = 0xee;
                let _ = put_one(&s, dummy, n as u32);
                n += 1;
                guard += 1;
                assert!(
                    guard < 80,
                    "could not roll oldest create into cold age (age={age} segs={} span={})",
                    s.txs.head.segment_count(),
                    SegmentedTxHead::soft_span_bytes()
                );
            }
            let new = put_one(&s, txid, 2);
            assert_ne!(old, new);
            let first = s.txs.head.first_fks_snapshot();
            let age_old = sealed_age_for_fk(&first, old.0).unwrap();
            let age_new = sealed_age_for_fk(&first, new.0).unwrap();
            assert!(
                age_old > HEAD_PROBE_HOT_MAX_AGE,
                "old fk must sit in cold age={age_old}"
            );
            assert!(
                age_new <= HEAD_PROBE_HOT_MAX_AGE,
                "new fk must sit in hot age={age_new}"
            );

            let mixed = [s.txs.secret.mix_txid(&txid)];
            let hot = s.txs.head.probe_candidates_batch_hot(&mixed).unwrap();
            let cold = s
                .txs
                .head
                .probe_candidates_batch_cold(&mixed, &[true])
                .unwrap();
            assert!(
                hot[0].iter().any(|f| *f == new) && !hot[0].iter().any(|f| *f == old),
                "hot={:?} new={new:?} old={old:?}",
                hot[0]
            );
            assert!(
                cold[0].iter().any(|f| *f == old) && !cold[0].iter().any(|f| *f == new),
                "cold={:?} old={old:?} new={new:?}",
                cold[0]
            );

            // Neither connected yet: newest unconnected (hot) for TipThenAny.
            assert_eq!(
                s.resolve_txid(&txid, TxidResolveMode::TipThenAny).unwrap(),
                Some(new)
            );
            assert_eq!(
                s.get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipThenAny)
                    .unwrap()[0]
                    .1
                    .map(|(f, _)| f),
                Some(new),
                "batch TipThenAny must keep newer unconnected when cold has no connected"
            );
            assert_eq!(
                s.get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipOnly)
                    .unwrap()[0]
                    .1,
                None
            );

            s.header_txs.put_range(Fk(1), old, 1).unwrap();
            s.confirmed.set(Height(0), Fk(1)).unwrap();
            s.rebuild_height_fence().unwrap();

            assert_eq!(
                s.resolve_txid(&txid, TxidResolveMode::TipOnly).unwrap(),
                Some(old)
            );
            assert_eq!(
                s.resolve_txid(&txid, TxidResolveMode::TipThenAny).unwrap(),
                Some(old)
            );
            let batch_tip = s
                .get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipOnly)
                .unwrap();
            assert_eq!(
                batch_tip[0].1.map(|(f, _)| f),
                Some(old),
                "TipOnly must take connected cold sibling, not unconnected hot"
            );
            let batch_any = s
                .get_fk_by_txid_batch_mode(&[txid], TxidResolveMode::TipThenAny)
                .unwrap();
            assert_eq!(
                batch_any[0].1.map(|(f, _)| f),
                Some(old),
                "TipThenAny must take connected cold sibling over newer unconnected hot"
            );
            let _ = std::fs::remove_dir_all(&dir);
        });
    }

    #[test]
    fn schema16_create_does_not_write_tx_height_and_fence_has_reorg_holes() {
        let dir = tmp();
        let s = Store::create(&dir).unwrap();
        assert!(
            !dir.join("tx_height.body").exists(),
            "schema 16 must not create tx_height.body"
        );
        s.header_txs.put_range(Fk(1), Fk(1), 2).unwrap();
        s.confirmed.set(Height(0), Fk(1)).unwrap();
        // Discarded block used fks 3..=5 under header 2 (not confirmed).
        s.header_txs.put_range(Fk(2), Fk(3), 3).unwrap();
        s.header_txs.put_range(Fk(3), Fk(6), 2).unwrap();
        s.confirmed.set(Height(1), Fk(3)).unwrap();
        s.rebuild_height_fence().unwrap();
        assert_eq!(s.tx_height_get(Fk(1)).unwrap(), Some(0));
        assert_eq!(s.tx_height_get(Fk(3)).unwrap(), None);
        assert_eq!(s.tx_height_get(Fk(5)).unwrap(), None);
        assert_eq!(s.tx_height_get(Fk(6)).unwrap(), Some(1));
        s.flush().unwrap();
        drop(s);
        assert!(!dir.join("tx_height.body").exists());
        // Leftover 15 file is unlinked on open.
        std::fs::write(dir.join("tx_height.body"), b"junk").unwrap();
        let s = Store::open(&dir).unwrap();
        assert!(!dir.join("tx_height.body").exists());
        assert_eq!(s.tx_height_get(Fk(5)).unwrap(), None);
        assert_eq!(s.tx_height_get(Fk(6)).unwrap(), Some(1));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
