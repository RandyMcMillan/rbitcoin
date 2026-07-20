//! Domain query layer over [`rbitcoin_store::Store`].

mod archive;
mod chain_view;
mod class_a_cache;
mod connect;
mod reconstruct;
mod scripthash;
mod point_run_builder;
mod sh_builder;
mod tip_prevout_cache;
mod tx_run_builder;
mod wave_prevout;

use bitcoin::absolute::LockTime;
use bitcoin::block::{Header as BlockHeader, Version as BlockVersion};
use bitcoin::consensus::Encodable;
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version as TxVersion;
use bitcoin::{
    Amount, Block, BlockHash, CompactTarget, OutPoint, ScriptBuf, Sequence, Transaction, TxIn,
    TxMerkleNode, TxOut, Witness,
};
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::{
    script_hash, HeaderRecord, IbdUtxo, InputRecord, OutputRecord, PointRecord, ScriptHashRecord,
    Store, StoreError, TxRecord,
};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Mutex;

pub type QueryError = StoreError;

pub use class_a_cache::stats as class_a_cache_stats;
pub use connect::ConfirmPrepared;
pub use tip_prevout_cache::stats as tip_prevout_cache_stats;
pub use wave_prevout::WavePrevoutCache;

/// Class C sub-phase wall times (nanoseconds; reset by the IBD sampler).
///
/// Split so logs can tell strong/height vs scripthash puts vs tip commit.
/// Scripthash subtimers (`SH_*`) break down collect vs durable append steps.
pub mod class_c_phase_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static STRONG_NS: AtomicU64 = AtomicU64::new(0);
    /// Wall time of the SH worker (collect + append), not including wait for strong.
    pub static SCRIPTHASH_NS: AtomicU64 = AtomicU64::new(0);
    pub static TIP_NS: AtomicU64 = AtomicU64::new(0);

    /// SH: warm create-tx index (first confirm only; should be ~0 after).
    pub static SH_WARM_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: filter which wave txs need create rows.
    pub static SH_FILTER_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: load creates from Class A for new txs.
    pub static SH_COLLECT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: sort creates by scripthash.
    pub static SH_SORT_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: seed process/durable heads for new scripthash keys.
    pub static SH_SEED_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: encode + body `write_at`.
    pub static SH_BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: `scripthash.head` insert_many.
    pub static SH_HEAD_NS: AtomicU64 = AtomicU64::new(0);
    /// SH: advance height watermark (was set inserts; now near-zero).
    pub static SH_INDEX_NS: AtomicU64 = AtomicU64::new(0);

    /// `(strong, scripthash, tip)` nanoseconds.
    ///
    /// `scripthash` is the **sum of SH substeps** (not a separate end-to-end
    /// timer), so status windows do not invent large `other_ms` when substeps
    /// and wall are sampled on different ticks.
    pub fn sample_and_reset() -> (u64, u64, u64) {
        (
            STRONG_NS.swap(0, Ordering::Relaxed),
            SCRIPTHASH_NS.swap(0, Ordering::Relaxed),
            TIP_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// `(warm, filter, collect, sort, seed, body, head, index)` nanoseconds.
    pub fn sample_sh_sub_and_reset() -> (u64, u64, u64, u64, u64, u64, u64, u64) {
        (
            SH_WARM_NS.swap(0, Ordering::Relaxed),
            SH_FILTER_NS.swap(0, Ordering::Relaxed),
            SH_COLLECT_NS.swap(0, Ordering::Relaxed),
            SH_SORT_NS.swap(0, Ordering::Relaxed),
            SH_SEED_NS.swap(0, Ordering::Relaxed),
            SH_BODY_NS.swap(0, Ordering::Relaxed),
            SH_HEAD_NS.swap(0, Ordering::Relaxed),
            SH_INDEX_NS.swap(0, Ordering::Relaxed),
        )
    }

    /// Accrue a SH substep and the aggregate `SCRIPTHASH_NS` wall (same window).
    #[inline]
    pub(crate) fn add_sh_part(part: &AtomicU64, ns: u64) {
        if ns == 0 {
            return;
        }
        part.fetch_add(ns, Ordering::Relaxed);
        SCRIPTHASH_NS.fetch_add(ns, Ordering::Relaxed);
    }
}

/// Connect prevout resolution counters (reset by the IBD sampler).
pub mod connect_prevout_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static TIP_HIT: AtomicU64 = AtomicU64::new(0);
    pub static WAVE_HIT: AtomicU64 = AtomicU64::new(0);
    pub static CLASS_A_HIT: AtomicU64 = AtomicU64::new(0);
    pub static STORE_MISS: AtomicU64 = AtomicU64::new(0);

    /// `(tip_hit, wave_hit, class_a_hit, store_miss)` then reset.
    pub fn sample_and_reset() -> (u64, u64, u64, u64) {
        (
            TIP_HIT.swap(0, Ordering::Relaxed),
            WAVE_HIT.swap(0, Ordering::Relaxed),
            CLASS_A_HIT.swap(0, Ordering::Relaxed),
            STORE_MISS.swap(0, Ordering::Relaxed),
        )
    }
}

/// Light UTXO diagnostics (reset by the IBD sampler).
pub mod ibd_utxo_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Confirm heal: apply failed → full `rebuild_ibd_utxo_to_tip`.
    pub static REBUILD_COUNT: AtomicU64 = AtomicU64::new(0);

    /// Rebuilds in the last sample window (then reset).
    pub fn sample_rebuilds_and_reset() -> u64 {
        REBUILD_COUNT.swap(0, Ordering::Relaxed)
    }

    #[inline]
    pub fn note_rebuild() {
        REBUILD_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

/// Wave-fill sub-phase wall times (nanoseconds; reset by the IBD sampler).
///
/// Breaks down the dominant `wave_fill` recon cost: body vs parent warm vs spent
/// vs coinbase height vs tip_prevout promote.
pub mod wave_fill_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Wave-body txs from Class A → wave map + parent_needed collect.
    pub static BODY_NS: AtomicU64 = AtomicU64::new(0);
    /// External parent `get_tx` (sorted-by-fk warm).
    pub static PARENT_TX_NS: AtomicU64 = AtomicU64::new(0);
    /// External parent output loads (sorted fk; full run or sparse).
    pub static PARENT_OUT_NS: AtomicU64 = AtomicU64::new(0);
    /// Durable / local spent filter on needed parent vouts.
    pub static SPENT_NS: AtomicU64 = AtomicU64::new(0);
    /// Coinbase create-height for parents.
    pub static CB_HEIGHT_NS: AtomicU64 = AtomicU64::new(0);
    /// Promote live parent slots into tip_prevout.
    pub static TIP_NOTE_NS: AtomicU64 = AtomicU64::new(0);

    /// `(body, parent_tx, parent_out, spent, cb_height, tip_note)` nanoseconds.
    pub fn sample_and_reset() -> (u64, u64, u64, u64, u64, u64) {
        (
            BODY_NS.swap(0, Ordering::Relaxed),
            PARENT_TX_NS.swap(0, Ordering::Relaxed),
            PARENT_OUT_NS.swap(0, Ordering::Relaxed),
            SPENT_NS.swap(0, Ordering::Relaxed),
            CB_HEIGHT_NS.swap(0, Ordering::Relaxed),
            TIP_NOTE_NS.swap(0, Ordering::Relaxed),
        )
    }

    #[inline]
    pub(crate) fn add(part: &AtomicU64, ns: u64) {
        if ns > 0 {
            part.fetch_add(ns, Ordering::Relaxed);
        }
    }
}


/// One transaction to apply when connecting a block.
#[derive(Clone, Debug)]
pub struct TxApply {
    pub tx: TxRecord,
    pub inputs: Vec<InputRecord>,
    pub outputs: Vec<OutputRecord>,
}

/// One header on the best store path after the confirmed tip (IBD resume).
#[derive(Clone, Debug)]
pub struct ResumeWorkEntry {
    pub height: u32,
    pub hash: [u8; 32],
    pub header_fk: Fk,
    /// True if `header_txs` has a body for this header (Class A ready).
    pub has_body: bool,
}

/// Domain query facade used by higher layers (consensus, net, RPC).
pub struct Query {
    store: Store,
    /// When false, archive **and** confirm skip durable Class B point (spend) writes.
    /// Catch-up spentness is light UTXO only; tip mode uses durable points + strong.
    /// Re-enable + [`Self::backfill_point_spends`] after catch-up (before Electrum).
    spend_index: std::sync::atomic::AtomicBool,
    /// When false, archive skips durable `tx.head` inserts (main archive cost).
    /// Parent resolve during catch-up uses light UTXO create_fk (not a txid map).
    tx_index: std::sync::atomic::AtomicBool,
    /// Process-local scripthash → body head fk (confirm append path; avoids durable chain walks).
    sh_heads: Mutex<HashMap<[u8; 32], Fk>>,
    /// Last height whose SH creates were enqueued/written **after tip commit**.
    /// `u64::MAX` = none. Replaces unbounded `sh_tx_indexed` HashSet.
    sh_indexed_through: AtomicU64,
    /// Byte-capped Class A working set (tx + runs) for confirm connect / reconstruct.
    class_a_cache: class_a_cache::ClassACache,
    /// Tip-window create txs + outputs filled **as we confirm** (and when
    /// resolving parents during connect). FIFO; independent of archive lead.
    tip_prevout_cache: tip_prevout_cache::TipPrevoutCache,
    /// Catch-up SH: memtable → sorted runs (no durable head on confirm).
    sh_run: sh_builder::ShRunBuilder,
    /// Catch-up tx.head via sorted runs.
    tx_run: tx_run_builder::TxRunBuilder,
    /// Catch-up point edges via sorted runs.
    point_run: point_run_builder::PointRunBuilder,
    /// Catch-up spentness: mmap unspent outpoint → create Class A fk.
    ibd_utxo: Mutex<Option<IbdUtxo>>,
}

impl Query {
    pub fn open_or_create(store_path: impl AsRef<Path>) -> Result<Self, QueryError> {
        let store = Store::open_or_create(store_path.as_ref())?;
        // Heal strong_tx / tx_height written above confirmed tip (kill -9 mid Class C).
        // Tip-bound spenders already ignore those rows; this restores is_strong parity.
        let repaired = store.repair_class_c_above_tip()?;
        if repaired > 0 {
            // Use eprintln so node logs still see it before rbitcoin_log is configured.
            eprintln!(
                "rbitcoin: repaired {repaired} Class C tx rows above confirmed tip (partial confirm / kill -9)"
            );
        }
        let store_path = store.path().to_path_buf();
        // SH watermark: on resume, assume 0..=tip already had SH work committed with tip.
        let sh_through = store
            .tip_height()
            .map(|h| h.0 as u64)
            .unwrap_or(u64::MAX);
        let q = Self {
            store,
            spend_index: std::sync::atomic::AtomicBool::new(true),
            tx_index: std::sync::atomic::AtomicBool::new(true),
            sh_heads: Mutex::new(HashMap::new()),
            sh_indexed_through: AtomicU64::new(sh_through),
            class_a_cache: class_a_cache::ClassACache::from_env(),
            tip_prevout_cache: tip_prevout_cache::TipPrevoutCache::from_env(),
            sh_run: sh_builder::ShRunBuilder::new(&store_path),
            tx_run: tx_run_builder::TxRunBuilder::new(&store_path),
            point_run: point_run_builder::PointRunBuilder::new(&store_path),
            ibd_utxo: Mutex::new(None),
        };
        // Warm cache from durable head if present (resume with index on).
        // Full body scan is not done here; fresh genesis IBD fills cache as it archives.
        Ok(q)
    }

    /// Last height with SH creates committed (after tip). `None` if empty chain.
    pub(crate) fn sh_indexed_through_height(&self) -> Option<u32> {
        let v = self.sh_indexed_through.load(AtomicOrdering::Acquire);
        if v == u64::MAX {
            None
        } else {
            Some(v as u32)
        }
    }

    /// Advance SH watermark only after Class C tip commit.
    pub(crate) fn set_sh_indexed_through_height(&self, height: Option<u32>) {
        let v = height.map(|h| h as u64).unwrap_or(u64::MAX);
        self.sh_indexed_through
            .store(v, AtomicOrdering::Release);
    }

    /// Open/create mmap IBD UTXO under the store dir. Aligns with store tip via
    /// replay/rebuild so resume skips a full chain walk when meta matches.
    pub fn enable_ibd_utxo(&self) -> Result<(), QueryError> {
        let mut g = self.ibd_utxo.lock().unwrap();
        if g.is_some() {
            return Ok(());
        }
        let mut u = IbdUtxo::open_or_create(self.store.path())?;
        let store_tip = self.tip_height().map(|h| h.0);
        // Empty live with a non-genesis tip is impossible (every height creates
        // ≥1 coinbase outpoint). Catches the pre-fix bug where multi-block
        // confirm advanced tip meta without inserting creates.
        let empty_poisoned = u.live_count() == 0 && u.tip().is_some();
        match (u.tip(), store_tip, empty_poisoned) {
            (_, _, true) => {
                u.clear()?;
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
            (t, s, _) if t == s => {
                // Consistent — ready without rebuild.
            }
            (Some(ut), Some(st), _) if ut < st => {
                *g = Some(u);
                drop(g);
                self.replay_ibd_utxo(ut + 1, st)?;
                return Ok(());
            }
            (None, Some(_), _) => {
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
            _ => {
                // meta ahead or mismatch — full rebuild
                u.clear()?;
                *g = Some(u);
                drop(g);
                self.rebuild_ibd_utxo_to_tip()?;
                return Ok(());
            }
        }
        *g = Some(u);
        Ok(())
    }

    pub fn ibd_utxo_enabled(&self) -> bool {
        self.ibd_utxo.lock().unwrap().is_some()
    }

    /// Snapshot for IBD perf: `(enabled, live_count, utxo_tip, rebuilds_this_window)`.
    ///
    /// `rebuilds_this_window` samples-and-resets [`ibd_utxo_stats::REBUILD_COUNT`].
    pub fn ibd_utxo_perf_snapshot(&self) -> (bool, u64, Option<u32>, u64) {
        let rebuilds = ibd_utxo_stats::sample_rebuilds_and_reset();
        let g = self.ibd_utxo.lock().unwrap();
        match *g {
            Some(ref u) => (true, u.live_count(), u.tip(), rebuilds),
            None => (false, 0, None, rebuilds),
        }
    }

    /// Count a confirm-path UTXO heal (apply failed → rebuild).
    pub fn note_ibd_utxo_rebuild(&self) {
        ibd_utxo_stats::note_rebuild();
    }

    /// True if outpoint is spent on the **catch-up** oracle (spend_index off).
    ///
    /// Light UTXO unspent set: not present ⇒ spent or never created.
    /// Tip mode (spend_index on) returns false so callers use durable points.
    pub fn catchup_is_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        if self.spend_index_enabled() {
            return Ok(false); // durable path handles spentness
        }
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Err(StoreError::Corrupt(
                "catch-up spentness requires light UTXO; call enable_ibd_utxo / enable_index_run_mode",
            ));
        };
        Ok(!u.contains(txid, vout)?)
    }

    /// After successful Class C: take spends, insert creates (with create fk), commit tip.
    ///
    /// Height-monotonic and idempotent: if `tip` is already reflected, no-op.
    /// Flushes mmap once (single-height / tests). Prefer [`Self::apply_ibd_utxo_run`]
    /// for multi-block confirm batches.
    pub fn apply_ibd_utxo_block(
        &self,
        spends: &[([u8; 32], u32)],
        creates: &[([u8; 32], u32, Fk)],
        tip: u32,
    ) -> Result<(), QueryError> {
        self.apply_ibd_utxo_run(&[(spends, creates, tip)])
    }

    /// Apply several heights in order (spends then creates per height), **one flush**.
    ///
    /// Each item is `(spends, creates, tip_height)`. Skips heights already ≤ UTXO tip.
    /// Critical for multi-block confirm: H creates must land before H+1 spends.
    pub fn apply_ibd_utxo_run(
        &self,
        steps: &[(
            & [([u8; 32], u32)],
            & [([u8; 32], u32, Fk)],
            u32,
        )],
    ) -> Result<(), QueryError> {
        if steps.is_empty() {
            return Ok(());
        }
        let mut g = self.ibd_utxo.lock().unwrap();
        let Some(ref mut u) = *g else {
            return Ok(());
        };
        let mut last_tip = u.tip();
        for &(spends, creates, tip) in steps {
            if let Some(have) = last_tip {
                if tip <= have {
                    continue;
                }
            }
            for &(txid, vout) in spends {
                let _ = u.take_spend(&txid, vout)?;
            }
            for &(txid, vout, create_fk) in creates {
                u.insert_create(&txid, vout, create_fk)?;
            }
            u.set_tip(Some(tip));
            last_tip = Some(tip);
        }
        u.flush()?;
        Ok(())
    }

    /// Unspent outpoint → create Class A fk (light UTXO). `None` if spent/missing
    /// or UTXO disabled.
    pub fn ibd_utxo_create_fk(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Result<Option<Fk>, QueryError> {
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Ok(None);
        };
        Ok(u.get_create_fk(txid, vout)?)
    }

    /// Rebuild mmap UTXO by replaying confirmed chain (creates then spends per height).
    pub fn rebuild_ibd_utxo_to_tip(&self) -> Result<u64, QueryError> {
        let mut g = self.ibd_utxo.lock().unwrap();
        let u = g.get_or_insert_with(|| {
            IbdUtxo::open_or_create(self.store.path()).expect("ibd utxo open")
        });
        u.clear()?;
        let Some(tip) = self.tip_height() else {
            u.commit_tip(None)?;
            return Ok(0);
        };
        drop(g);
        self.replay_ibd_utxo(0, tip.0)?;
        Ok(self
            .ibd_utxo
            .lock()
            .unwrap()
            .as_ref()
            .map(|u| u.live_count())
            .unwrap_or(0))
    }

    fn replay_ibd_utxo(&self, from_h: u32, to_h: u32) -> Result<(), QueryError> {
        for h in from_h..=to_h {
            let fks = match self.block_tx_fks(Height(h)) {
                Ok(f) => f,
                Err(StoreError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            let mut g = self.ibd_utxo.lock().unwrap();
            let u = g.as_mut().ok_or(StoreError::Corrupt("ibd utxo missing"))?;
            // Per-tx order: spends then creates (same-block chain of custody).
            for fk in fks {
                let tx = self.store.get_tx(fk)?;
                if tx.input_count > 0 {
                    let inputs = self.tx_input_run(&tx)?;
                    for inp in &inputs {
                        if inp.is_coinbase() {
                            continue;
                        }
                        let prev = self.resolve_prev_txid(inp)?;
                        if !u.take_spend(&prev, inp.prev_index)? {
                            return Err(StoreError::Corrupt("ibd utxo rebuild take failed"));
                        }
                    }
                }
                for v in 0..tx.output_count {
                    u.insert_create(&tx.txid, v, fk)?;
                }
            }
            u.commit_tip(Some(h))?;
        }
        Ok(())
    }

    /// Hard-block catch-up confirm unless light UTXO is enabled and tip-aligned.
    ///
    /// Tip mode (spend_index on) uses durable points only — always ready.
    pub fn ensure_spent_oracle_ready(&self) -> Result<(), QueryError> {
        if self.spend_index_enabled() {
            return Ok(());
        }
        let chain_tip = self.tip_height().map(|h| h.0);
        let g = self.ibd_utxo.lock().unwrap();
        let Some(ref u) = *g else {
            return Err(StoreError::Corrupt(
                "catch-up requires light UTXO; call enable_ibd_utxo / enable_index_run_mode",
            ));
        };
        if u.tip() != chain_tip {
            return Err(StoreError::Corrupt(
                "light UTXO tip not aligned with chain tip; call rebuild_ibd_utxo_to_tip",
            ));
        }
        Ok(())
    }

    /// Rebuild catch-up spentness oracle to tip (light UTXO).
    pub fn rebuild_spent_oracle_to_tip(&self) -> Result<u64, QueryError> {
        self.rebuild_ibd_utxo_to_tip()
    }

    /// Catch-up: SH / tx / point durable open-hash off; sequential runs + mmap UTXO.
    pub fn enable_index_run_mode(&self) -> Result<(), QueryError> {
        self.sh_run.enable();
        self.tx_run.enable();
        self.point_run.enable();
        self.enable_ibd_utxo()?;
        Ok(())
    }

    /// Flush/compact SH runs and bulk-load durable scripthash tables (tip mode).
    pub fn finalize_sh_runs(&self) -> Result<u64, QueryError> {
        self.sh_run.finalize_and_materialize(&self.store)
    }

    pub fn finalize_tx_runs(&self) -> Result<u64, QueryError> {
        self.tx_run.finalize_and_materialize(&self.store)
    }

    pub fn finalize_point_runs(&self) -> Result<u64, QueryError> {
        self.point_run.finalize_and_materialize(&self.store)
    }

    pub(crate) fn tx_run_enabled(&self) -> bool {
        self.tx_run.is_enabled()
    }

    pub(crate) fn point_run_enabled(&self) -> bool {
        self.point_run.is_enabled()
    }

    pub(crate) fn enqueue_tx_run(&self, txid: [u8; 32], fk: Fk) {
        self.tx_run.enqueue(txid, fk);
    }

    pub(crate) fn enqueue_point_run_edges(
        &self,
        edges: &[( [u8; 32], u32, Fk, u32, u32)],
    ) {
        self.point_run.enqueue_batch(edges);
    }

    /// Resolve txid → fk: durable `tx.head` when index on; else `tx.runs` if enabled.
    /// Catch-up parent resolve prefers light UTXO create_fk (outpoint), not this.
    fn lookup_tx_fk(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        if self.tx_index_enabled() {
            if let Some((fk, _)) = self.store.get_tx_by_txid(txid)? {
                return Ok(Some(fk));
            }
        }
        if self.tx_run_enabled() {
            return self.tx_run.lookup(txid);
        }
        Ok(None)
    }

    /// Public resolve by txid (durable head / runs). Prefer `ibd_utxo_create_fk` for unspent.
    pub fn tx_fk_by_txid(&self, txid: &[u8; 32]) -> Result<Option<Fk>, QueryError> {
        self.lookup_tx_fk(txid)
    }

    /// Linear Class A body scan for txid → fk (disconnect undo without head/runs).
    /// O(n) — rare reorg path only.
    pub(crate) fn find_tx_fk_by_txid_scan(
        &self,
        txid: &[u8; 32],
    ) -> Result<Option<Fk>, QueryError> {
        let n = self.store.txs.count();
        for id in 1..=n {
            let fk = Fk(id);
            let rec = self.store.get_tx(fk)?;
            if &rec.txid == txid {
                return Ok(Some(fk));
            }
        }
        Ok(None)
    }

    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Class A working-set cache size `(entries, approx_bytes, budget_bytes)`.
    pub fn class_a_cache_usage(&self) -> (usize, usize, usize) {
        (
            self.class_a_cache.len(),
            self.class_a_cache.approx_bytes(),
            self.class_a_cache.budget_bytes(),
        )
    }

    pub fn tip_prevout_cache_usage(&self) -> (usize, usize, usize) {
        (
            self.tip_prevout_cache.len(),
            self.tip_prevout_cache.approx_bytes(),
            self.tip_prevout_cache.budget_bytes(),
        )
    }

    /// Remember a create tx + outputs in the tip-window prevout cache.
    pub(crate) fn tip_prevout_note(
        &self,
        fk: Fk,
        tx: TxRecord,
        outputs: Vec<OutputRecord>,
    ) {
        self.tip_prevout_cache.note(fk, tx, outputs);
    }

    /// Single-lock tip_prevout resolve for connect: `(tx, output)`.
    pub fn tip_prevout_tx_and_output(
        &self,
        fk: Fk,
        vout: u32,
    ) -> Option<(TxRecord, OutputRecord)> {
        self.tip_prevout_cache.get_tx_and_output_at(fk, vout)
    }

    /// Single-lock tip_prevout resolve by parent txid.
    pub fn tip_prevout_tx_and_output_by_txid(
        &self,
        txid: &[u8; 32],
        vout: u32,
    ) -> Option<(Fk, TxRecord, OutputRecord)> {
        self.tip_prevout_cache
            .get_tx_and_output_by_txid(txid, vout)
    }

    /// After successful Class C: drop spent vouts from tip_prevout (budget reclaim).
    pub fn retire_tip_prevout_spends(&self, spends: &[([u8; 32], u32)]) {
        self.tip_prevout_cache.retire_spends(spends);
    }

    /// True if outpoint is cached as live unspent in tip_prevout (write-through).
    pub fn tip_prevout_has_live(&self, txid: &[u8; 32], vout: u32) -> bool {
        self.tip_prevout_cache.has_live_output_txid(txid, vout)
    }

    /// True if outpoint is cached as live unspent in tip_prevout by create fk.
    pub fn tip_prevout_has_live_fk(&self, fk: Fk, vout: u32) -> bool {
        self.tip_prevout_cache.has_live_output(fk, vout)
    }

    /// (D) When archive leads tip by more than this many bodies, skip bulk Class A
    /// cache fill on archive. Confirm-wave **prefetch** (A) warms tip+1…N instead.
    /// Tip-follow (small lead) fills from archive so tip+1 is warm at write time.
    ///
    /// Override with `RBITCOIN_CLASS_A_ARCHIVE_LEAD` (block count).
    pub(crate) fn class_a_archive_fill_max_lead(&self) -> u64 {
        std::env::var("RBITCOIN_CLASS_A_ARCHIVE_LEAD")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(512)
    }

    /// True when archive is near the confirmed tip (or tip not established).
    pub(crate) fn should_fill_class_a_from_archive(&self) -> bool {
        let max_lead = self.class_a_archive_fill_max_lead();
        let tip = match self.tip_height() {
            Some(h) => h.0 as u64,
            // Pre-genesis confirm: archive may already race ahead — don't flood.
            None => return false,
        };
        let bodies = self.store.archived_block_count().unwrap_or(0);
        // bodies includes height 0..; tip is last confirmed height.
        // lead ≈ archived heights above tip.
        let lead = bodies.saturating_sub(tip.saturating_add(1));
        lead <= max_lead
    }

    /// No-op compatibility: SH dedupe is a height watermark (`sh_indexed_through`),
    /// not a HashSet warm from durable body.
    pub fn warm_scripthash_create_index(&self) -> Result<(), QueryError> {
        // Align watermark with tip if durable SH exists (tip mode resume).
        if let Some(tip) = self.tip_height() {
            if self.sh_indexed_through_height().is_none() {
                self.set_sh_indexed_through_height(Some(tip.0));
            }
        }
        Ok(())
    }

    /// Enable/disable durable point (spend) multimap writes on archive **and** confirm
    /// (default on). Off during catch-up for speed; re-enable and materialize /
    /// [`Self::backfill_point_spends`] before Electrum.
    ///
    /// When turning **off**, catch-up confirm requires light UTXO
    /// ([`Self::enable_ibd_utxo`] / [`Self::enable_index_run_mode`]).
    pub fn set_spend_index(&self, enabled: bool) {
        self.spend_index
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn spend_index_enabled(&self) -> bool {
        self.spend_index
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Buffer `point.head` upserts in process RAM during full-validation IBD.
    ///
    /// Writes spill sorted/page-buffered when the map reaches `max_entries` or on
    /// store flush. Cuts continuous random RMW on multi‑GiB `point.head`.
    pub fn enable_point_head_write_behind(&self, max_entries: usize) -> Result<(), QueryError> {
        self.store.enable_point_head_write_behind(max_entries)
    }

    pub fn disable_point_head_write_behind(&self) -> Result<(), QueryError> {
        self.store.disable_point_head_write_behind()
    }

    pub fn spill_point_head(&self) -> Result<(), QueryError> {
        self.store.spill_point_head()
    }

    /// Budgeted spill of `point.head` overlay (≤ `max_entries` keys).
    pub fn spill_point_head_budget(&self, max_entries: usize) -> Result<usize, QueryError> {
        self.store.spill_point_head_budget(max_entries)
    }

    /// Defer soft-cap point.head spills during confirm connect.
    /// Clearing defer does not bulk-spill (background + archive drain).
    pub fn set_point_head_defer_spill(&self, defer: bool) -> Result<(), QueryError> {
        self.store.set_point_head_defer_spill(defer)
    }

    /// Buffer `tx.head` upserts (optional; useful when durable tx index is on).
    pub fn enable_tx_head_write_behind(&self, max_entries: usize) -> Result<(), QueryError> {
        self.store.enable_tx_head_write_behind(max_entries)
    }

    pub fn disable_tx_head_write_behind(&self) -> Result<(), QueryError> {
        self.store.disable_tx_head_write_behind()
    }

    pub fn spill_tx_head(&self) -> Result<(), QueryError> {
        self.store.spill_tx_head()
    }

    /// Budgeted spill of `tx.head` overlay (≤ `max_entries` keys).
    pub fn spill_tx_head_budget(&self, max_entries: usize) -> Result<usize, QueryError> {
        self.store.spill_tx_head_budget(max_entries)
    }

    /// Defer soft-cap tx.head spills during confirm.
    /// Clearing defer does not bulk-spill (background + archive drain).
    pub fn set_tx_head_defer_spill(&self, defer: bool) -> Result<(), QueryError> {
        self.store.set_tx_head_defer_spill(defer)
    }

    /// One short-slice step on both head overlays (background worker / archive).
    pub fn spill_heads_step_if_needed(&self) -> Result<(usize, usize), QueryError> {
        self.store.spill_heads_step_if_needed()
    }

    /// Host-friendly process-exit flush (see [`rbitcoin_store::Store::flush_for_shutdown`]).
    pub fn flush_for_shutdown(&self) -> Result<(), QueryError> {
        self.store.flush_for_shutdown()
    }
}

impl Query {
    /// True if this outpoint is spent on the **best chain**.
    ///
    /// - **Catch-up** (`spend_index` off): light UTXO unspent set.
    /// - **Tip** (`spend_index` on): durable confirmed-strong point edges only.
    ///
    /// Does **not** treat archive-only point rows as spent: Class A may write
    /// edges before Class C; those spenders are not strong yet.
    pub fn is_outpoint_spent(&self, txid: &[u8; 32], vout: u32) -> Result<bool, QueryError> {
        if !self.spend_index_enabled() {
            return self.catchup_is_spent(txid, vout);
        }
        self.store.has_confirmed_strong_spender(txid, vout)
    }

    /// Enable/disable txid hash-head inserts on archive (default on). Off under
    /// milestone IBD; Class A bodies remain complete via header_txs fk lists.
    pub fn set_tx_index(&self, enabled: bool) {
        self.tx_index
            .store(enabled, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn tx_index_enabled(&self) -> bool {
        self.tx_index
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Write durable `tx.head` for every Class A body missing from the hash head.
    ///
    /// After milestone IBD (`tx_index` off), Electrum and prevout-by-txid need this
    /// before scripthash backfill / `transaction.get`. Idempotent. Returns inserts.
    ///
    /// `on_progress(done_bodies, total_bodies, inserted)` for operator logs.
    pub fn backfill_tx_index(
        &self,
        on_progress: impl FnMut(u64, u64, u64),
    ) -> Result<u64, QueryError> {
        self.store.txs.backfill_head(on_progress)
    }

    /// Class A tx body count (for backfill heuristics / logs).
    pub fn tx_body_count(&self) -> u64 {
        self.store.txs.count()
    }

    /// Durable `tx.head` occupied slots (for backfill heuristics / logs).
    pub fn tx_head_occupied(&self) -> u64 {
        self.store.txs.head_occupied()
    }

    /// Thin scripthash create row count (diagnostic / tip-mode logs).
    pub fn scripthash_entry_count(&self) -> u64 {
        self.store.scripthash.entry_count()
    }

    /// Durable point (spend-edge) count (for backfill heuristics / logs).
    pub fn point_edge_count(&self) -> u64 {
        self.store.points.edge_count()
    }

    /// Write durable point edges for every confirmed non-coinbase input.
    ///
    /// After milestone IBD (confirm skipped `put_spend`), Electrum and
    /// `spenders()` need this. When the point table is empty, uses an append-only
    /// bulk path (`put_spend_batch`, no `spenders_raw` probe). Otherwise probes
    /// for idempotency.
    ///
    /// `on_progress(height, tip, txs_so_far, edges_so_far)`.
    /// Returns `(heights_walked, txs_touched)`.
    pub fn backfill_point_spends(
        &self,
        mut on_progress: impl FnMut(u32, u32, u64, u64),
    ) -> Result<(u32, u64), QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok((0, 0));
        };
        // Empty index → bulk append. Sparse/partial → probe to avoid dups.
        let probe = self.point_edge_count() > 0;
        let mut txs = 0u64;
        let mut edges_total = 0u64;
        const EDGE_BATCH: usize = 8192;
        const PROGRESS_EVERY: u32 = 10_000;
        let mut edge_batch: Vec<([u8; 32], u32, Fk, u32)> = Vec::with_capacity(EDGE_BATCH);
        let mut last_log = 0u32;

        let flush_batch = |batch: &mut Vec<([u8; 32], u32, Fk, u32)>| -> Result<(), QueryError> {
            if batch.is_empty() {
                return Ok(());
            }
            self.store.put_spend_batch(batch)?;
            batch.clear();
            Ok(())
        };

        for h in 0..=tip.0 {
            let height = Height(h);
            let fks = match self.block_tx_fks(height) {
                Ok(f) => f,
                Err(StoreError::NotFound) => continue,
                Err(e) => return Err(e),
            };
            for fk in fks {
                if probe {
                    // Per-tx path with existence probe (partial reindex).
                    self.mark_spends_for_tx(fk, true)?;
                } else {
                    let mut edges = self.collect_spend_edges(fk, false)?;
                    edges_total += edges.len() as u64;
                    if edge_batch.len() + edges.len() > EDGE_BATCH && !edge_batch.is_empty() {
                        flush_batch(&mut edge_batch)?;
                    }
                    if edges.len() >= EDGE_BATCH {
                        // Single fat tx: write on its own.
                        self.store.put_spend_batch(&edges)?;
                    } else {
                        edge_batch.append(&mut edges);
                        if edge_batch.len() >= EDGE_BATCH {
                            flush_batch(&mut edge_batch)?;
                        }
                    }
                }
                txs += 1;
            }
            if h - last_log >= PROGRESS_EVERY || h == tip.0 {
                on_progress(h, tip.0, txs, edges_total + edge_batch.len() as u64);
                last_log = h;
            }
        }
        flush_batch(&mut edge_batch)?;
        Ok((tip.0.saturating_add(1), txs))
    }

    pub fn tip_height(&self) -> Option<Height> {
        self.store.tip_height()
    }

    pub fn tip_header_fk(&self) -> Result<Option<Fk>, QueryError> {
        match self.tip_height() {
            None => Ok(None),
            Some(h) => Ok(self.store.confirmed.get(h)?),
        }
    }

    pub fn put_header(&self, rec: &HeaderRecord) -> Result<Fk, QueryError> {
        self.store.put_header(rec)
    }

    pub fn get_header(&self, fk: Fk) -> Result<HeaderRecord, QueryError> {
        self.store.get_header(fk)
    }

    pub fn get_header_by_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Fk, HeaderRecord)>, QueryError> {
        self.store.get_header_by_hash(hash)
    }

    pub fn put_tx(&self, rec: &TxRecord) -> Result<Fk, QueryError> {
        self.store.put_tx(rec)
    }

    pub fn get_tx(&self, fk: Fk) -> Result<TxRecord, QueryError> {
        // Tip-window first (confirm prevout locality), then archive-filled Class A.
        if let Some(tx) = self.tip_prevout_cache.get_tx(fk) {
            return Ok(tx);
        }
        self.get_tx_class_a(fk)
    }

    /// Class A → store only. Reconstruct / bulk Class C / connect cold path use
    /// this so `tip_prevout` hit rates reflect intentional prevout probes.
    pub fn get_tx_class_a(&self, fk: Fk) -> Result<TxRecord, QueryError> {
        if let Some(tx) = self.class_a_cache.get_tx(fk) {
            return Ok(tx);
        }
        let tx = self.store.get_tx(fk)?;
        // Cache tx row only; runs filled on demand by tx_*_run / reconstruct.
        self.class_a_cache.note(fk, tx.clone(), None, None);
        Ok(tx)
    }

    pub fn get_tx_by_txid(&self, txid: &[u8; 32]) -> Result<Option<(Fk, TxRecord)>, QueryError> {
        if let Some(fk) = self.lookup_tx_fk(txid)? {
            return Ok(Some((fk, self.get_tx(fk)?)));
        }
        Ok(None)
    }

    pub fn put_output_run(&self, recs: &[OutputRecord]) -> Result<Fk, QueryError> {
        self.store.put_output_run(recs)
    }

    pub fn get_output_at(
        &self,
        run_fk: Fk,
        count: u32,
        index: u32,
    ) -> Result<OutputRecord, QueryError> {
        self.store.get_output_at(run_fk, count, index)
    }

    pub fn get_output_run(&self, run_fk: Fk, count: u32) -> Result<Vec<OutputRecord>, QueryError> {
        self.store.get_output_run(run_fk, count)
    }

    pub fn put_input_run(&self, recs: &[InputRecord]) -> Result<Fk, QueryError> {
        self.store.put_input_run(recs)
    }

    pub fn get_input_at(
        &self,
        run_fk: Fk,
        count: u32,
        index: u32,
    ) -> Result<InputRecord, QueryError> {
        self.store.get_input_at(run_fk, count, index)
    }

    pub fn get_input_run(&self, run_fk: Fk, count: u32) -> Result<Vec<InputRecord>, QueryError> {
        self.store.get_input_run(run_fk, count)
    }

    /// Input `i` of a tx row (run-addressed).
    pub fn tx_input(&self, tx: &TxRecord, i: u32) -> Result<InputRecord, QueryError> {
        if i >= tx.input_count {
            return Err(StoreError::NotFound);
        }
        let run = tx.input_start_fk.get().ok_or(StoreError::InvalidFk)?;
        self.get_input_at(Fk(run), tx.input_count, i)
    }

    /// Output `vout` of a tx row (run-addressed).
    pub fn tx_output(&self, tx: &TxRecord, vout: u32) -> Result<OutputRecord, QueryError> {
        self.tx_output_attributed(tx, vout, false)
    }

    /// Like [`Self::tx_output`] but records connect cold-path counters when
    /// `count_connect` is true. When true, **skips tip_prevout probe** (caller
    /// already tried the single-lock fast path) to avoid double MISS stats.
    pub fn tx_output_attributed(
        &self,
        tx: &TxRecord,
        vout: u32,
        count_connect: bool,
    ) -> Result<OutputRecord, QueryError> {
        use std::sync::atomic::Ordering;
        if vout >= tx.output_count {
            return Err(StoreError::NotFound);
        }
        // Prefer full-run cache via fk when we know it (txid→fk process cache).
        if let Some(fk) = self.lookup_tx_fk(&tx.txid)? {
            if !count_connect {
                if let Some(o) = self.tip_prevout_cache.get_output_at(fk, vout) {
                    return Ok(o);
                }
            }
            if let Some(o) = self.class_a_cache.get_output_at(fk, vout) {
                if count_connect {
                    connect_prevout_stats::CLASS_A_HIT.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(o);
            }
            // Load full run once into cache (connect often probes many vouts).
            let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
            let outs = self.get_output_run(Fk(run), tx.output_count)?;
            let out = outs
                .get(vout as usize)
                .cloned()
                .ok_or(StoreError::NotFound)?;
            // Promote resolved creates into tip-window (prevout path).
            self.tip_prevout_cache
                .note(fk, tx.clone(), outs.clone());
            self.class_a_cache.fill_outputs(fk, outs);
            if count_connect {
                connect_prevout_stats::STORE_MISS.fetch_add(1, Ordering::Relaxed);
            }
            return Ok(out);
        }
        let run = tx.output_start_fk.get().ok_or(StoreError::InvalidFk)?;
        let out = self.get_output_at(Fk(run), tx.output_count, vout)?;
        if count_connect {
            connect_prevout_stats::STORE_MISS.fetch_add(1, Ordering::Relaxed);
        }
        Ok(out)
    }

    pub fn put_spend(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
        spending_tx_fk: Fk,
        spending_input_index: u32,
    ) -> Result<Fk, QueryError> {
        self.store
            .put_spend(out_txid, out_index, spending_tx_fk, spending_input_index)
    }

    /// Strong (best-chain confirmed) spenders only.
    pub fn spenders(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders(out_txid, out_index)
    }

    pub fn spenders_raw(
        &self,
        out_txid: &[u8; 32],
        out_index: u32,
    ) -> Result<Vec<PointRecord>, QueryError> {
        self.store.spenders_raw(out_txid, out_index)
    }

    /// True if this header hash has a Class A row (may not be confirmed on tip).
    pub fn is_header_archived(&self, hash: &[u8; 32]) -> Result<bool, QueryError> {
        Ok(self.get_header_by_hash(hash)?.is_some())
    }

    /// True if the full block body is in Class A (`header_txs` present).
    ///
    /// Does **not** walk the confirmed chain (that was O(tip) per call and froze
    /// IBD when thousands of header-only rows existed). Callers that need
    /// "confirmed or archived" should check the confirmed set / tip first.
    pub fn is_block_archived(&self, hash: &[u8; 32]) -> Result<bool, QueryError> {
        let Some((fk, _)) = self.get_header_by_hash(hash)? else {
            return Ok(false);
        };
        Ok(self.store.header_txs.has_body(fk)?)
    }

    /// Total headers with a Class A body on disk (durable, any prior run).
    pub fn archived_block_count(&self) -> Result<u64, QueryError> {
        Ok(self.store.archived_block_count()?)
    }

    /// Rebuild the post-tip work path from durable headers + Class A bodies.
    ///
    /// Parallel IBD only remembered the ordered path in RAM. On restart it re-ran
    /// getheaders/getdata even though Class A was already on disk. This walks
    /// `header.body` once, builds a prev→children map, and follows the best
    /// (prefer-archived) child chain from the confirmed tip.
    ///
    /// `max` caps how many headers are returned (IBD ordered cap).
    pub fn resume_work_path_after_tip(
        &self,
        tip_hash: [u8; 32],
        tip_height: u32,
        max: usize,
    ) -> Result<Vec<ResumeWorkEntry>, QueryError> {
        if max == 0 {
            return Ok(Vec::new());
        }
        let Some((tip_fk, _)) = self.get_header_by_hash(&tip_hash)? else {
            return Ok(Vec::new());
        };
        let n = self.store.header_count();
        if n == 0 {
            return Ok(Vec::new());
        }

        // prev_fk → list of (child_fk, child_hash)
        let mut children: HashMap<u64, Vec<(Fk, [u8; 32])>> = HashMap::new();
        for id in 1..=n {
            let fk = Fk(id);
            let rec = self.store.get_header(fk)?;
            let prev = rec.prev_fk.get().unwrap_or(0);
            children.entry(prev).or_default().push((fk, rec.hash));
        }

        let mut out = Vec::with_capacity(max.min(4096));
        let mut cur_fk = tip_fk;
        let mut height = tip_height;
        while out.len() < max {
            let Some(kids) = children.get(&cur_fk.0) else {
                break;
            };
            if kids.is_empty() {
                break;
            }
            // Prefer a child that already has a Class A body; among ties, highest fk
            // (later archive / main-chain append order).
            let mut best: Option<(Fk, [u8; 32], bool)> = None;
            for &(fk, hash) in kids {
                let has_body = self.store.header_txs.has_body(fk)?;
                let take = match best {
                    None => true,
                    Some((best_fk, _, best_body)) => {
                        (has_body && !best_body) || (has_body == best_body && fk.0 > best_fk.0)
                    }
                };
                if take {
                    best = Some((fk, hash, has_body));
                }
            }
            let Some((fk, hash, has_body)) = best else {
                break;
            };
            height = height.saturating_add(1);
            out.push(ResumeWorkEntry {
                height,
                hash,
                header_fk: fk,
                has_body,
            });
            cur_fk = fk;
        }
        Ok(out)
    }

    /// Flush header rows + Class A body associations (IBD writer durability).
    pub fn flush_header_archive(&self) -> Result<(), QueryError> {
        Ok(self.store.flush_header_archive()?)
    }

    /// Ensure a header row exists (no txs). Idempotent by hash.
    ///
    /// Used to pipeline header sync into the store so out-of-order bodies can
    /// resolve `prev_fk` without waiting for tip confirm.
    pub fn ensure_header(&self, header: &HeaderRecord) -> Result<Fk, QueryError> {
        if let Some((fk, _)) = self.get_header_by_hash(&header.hash)? {
            return Ok(fk);
        }
        Ok(self.store.put_header(header)?)
    }

    pub fn flush(&self) -> Result<(), QueryError> {
        if !self.store.path().exists() {
            return Err(StoreError::NotDirectory(self.store.path().to_path_buf()));
        }
        self.store.flush()
    }
}

fn wire_header(rec: &HeaderRecord, prev_blockhash: BlockHash) -> BlockHeader {
    BlockHeader {
        version: BlockVersion::from_consensus(rec.version),
        prev_blockhash,
        merkle_root: TxMerkleNode::from_byte_array(rec.merkle_root),
        time: rec.timestamp,
        bits: CompactTarget::from_consensus(rec.bits),
        nonce: rec.nonce,
    }
}

/// Electrum `blockchain.scripthash.get_history` row (confirmed only in v1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashHistoryItem {
    pub height: i64,
    pub txid: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashBalance {
    pub confirmed: i64,
    pub unconfirmed: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScriptHashUtxo {
    pub tx_hash: [u8; 32],
    pub tx_pos: u32,
    pub height: u32,
    pub value: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MerkleProof {
    pub block_height: u32,
    pub pos: usize,
    pub merkle: Vec<[u8; 32]>,
}

pub fn crate_name() -> &'static str {
    "rbitcoin-query"
}
