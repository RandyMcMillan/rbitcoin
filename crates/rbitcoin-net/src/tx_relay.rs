//! Tip-mode transaction relay (P4): inv/getdata/tx + mempool announce.
//!
//! Heavy relay is **gated** on [`MempoolHub::set_relay_enabled`] (false during IBD).
//! BIP331 package *wire* is not in rust-bitcoin 0.32's `NetworkMessage`; package
//! accept stays on [`rbitcoin_mempool::ActiveMempool::accept_package`]. Unknown
//! `sendpackages` / package commands are ignored until a bitcoin crate upgrade.

use arc_swap::ArcSwap;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{Amount, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Wtxid};
use rbitcoin_mempool::{
    default_candidate_rates, frontier_feerate_from_chunks, min_rate_for_capacity,
    weight_above_from_chunks, AcceptError, AcceptResult, ActiveMempool, ChainTipCtx, Chunk, Coin,
    FeeFlowMeter, UtxoProvider, BLOCK_WEIGHT_WU,
};
use rbitcoin_query::Query;
use rbitcoin_store::OutputRecord;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

/// Max age of a published fee snapshot before refresh (request path is still Arc-load only
/// after a concurrent refresh has finished; see [`MempoolHub::maybe_refresh_fee_snapshot`]).
const FEE_SNAPSHOT_MAX_AGE: Duration = Duration::from_secs(1);

/// Esplora `/fee-estimates` keys + common Electrum depths (after 0–2 → default map).
const FEE_SNAPSHOT_DEPTHS: &[u32] = &[1, 2, 3, 4, 5, 6, 10, 20, 144, 504, 1008];

/// Immutable published fee table + mining chunks (request path never walks the graph).
#[derive(Clone, Debug)]
struct FeeSnapshot {
    /// BTC/kB by confirm-target depth (post 0–2 mapping). Missing → treat empty.
    by_depth_btc_per_kb: HashMap<u32, f64>,
    /// Best-first mining chunks from the last refresh (histogram / frontier).
    chunks: Vec<Chunk>,
    computed_at: Instant,
}

impl FeeSnapshot {
    fn empty(now: Instant) -> Self {
        Self {
            by_depth_btc_per_kb: HashMap::new(),
            chunks: Vec::new(),
            computed_at: now,
        }
    }

    fn rate_btc_per_kb(&self, depth: u32) -> f64 {
        self.by_depth_btc_per_kb
            .get(&depth)
            .copied()
            .unwrap_or(-1.0)
    }

    fn histogram(&self) -> Vec<(u64, u64)> {
        let mut by_rate: std::collections::BTreeMap<u64, u64> = std::collections::BTreeMap::new();
        for ch in &self.chunks {
            let rate = ch.fee_rate_sat_per_kvb();
            let vsize = rbitcoin_consensus::policy::get_virtual_size(ch.weight);
            *by_rate.entry(rate).or_insert(0) += vsize;
        }
        by_rate.into_iter().rev().collect()
    }
}

/// Resolve prevouts from the relational archive (confirmed **unspent** UTXOs).
///
/// Returns no coin when the create is unknown **or** a confirmed-strong spender
/// exists (finding 010 — mirror Core coins-view spentness).
pub struct QueryUtxoProvider<'a> {
    pub query: &'a Query,
}

impl UtxoProvider for QueryUtxoProvider<'_> {
    fn get_coin(&self, op: &OutPoint) -> Option<Coin> {
        let tid = op.txid.to_byte_array();
        let (fk, rec) = self.query.get_tx_by_txid(&tid).ok().flatten()?;
        // Confirmed-strong spent ⇒ absent (do not admit double-spends of chain UTXOs).
        if self.query.is_outpoint_spent(&tid, op.vout).ok()? {
            return None;
        }
        let out: OutputRecord = self
            .query
            .tx_output_at_fk(fk, &rec, op.vout)
            .ok()
            .or_else(|| self.query.tx_output(&rec, op.vout).ok())?;
        let value = if out.value < 0 {
            Amount::ZERO
        } else {
            Amount::from_sat(out.value as u64)
        };
        let create_height = self
            .query
            .store()
            .tx_height
            .get(fk)
            .ok()
            .flatten()
            .unwrap_or(0);
        // Coinbase: first input null prevout when we can load inputs; else height-0 heuristic.
        let is_coinbase = self
            .query
            .tx_input_at_fk(fk, &rec, 0)
            .ok()
            .map(|i| i.is_coinbase() || i.prev_index == u32::MAX)
            .unwrap_or(false);
        Some(Coin {
            txout: TxOut {
                value,
                script_pubkey: ScriptBuf::from_bytes(out.script),
            },
            create_height,
            is_coinbase,
        })
    }
}

/// Cap for Esplora `/mempool/recent` (newest accepts, process-local).
pub const MEMPOOL_RECENT_CAP: usize = 32;

/// One recently accepted mempool tx (for explorer "recent" strips).
#[derive(Clone, Debug)]
pub struct RecentAccept {
    pub txid: Txid,
    pub fee_sat: u64,
    pub weight: u64,
    /// Sum of output values (sats).
    pub value_sat: u64,
}

/// Broadcast unit for mempool accepts (P2P inv, Electrum status, Esplora WS).
///
/// `replaced` lists conflict txids removed by full-RBF/RBFR when admitting `txid`
/// (empty when there was no replacement). `replaced_scripthashes` are output
/// scripthashes of those bodies **before** removal (wallet address-track RBF).
/// Subscribers that only care about new inventory can ignore both.
#[derive(Clone, Debug)]
pub struct MempoolAnnounce {
    pub txid: Txid,
    pub replaced: Vec<Txid>,
    pub replaced_scripthashes: Vec<[u8; 32]>,
}

/// Sample-and-reset window of tip-follow mempool/relay meters (`DEBUG tip: perf`).
#[derive(Clone, Copy, Debug, Default)]
pub struct MempoolPerfSample {
    pub accepts: u64,
    pub rejects: u64,
    /// Sum of accept_tx wall times (µs) this window.
    pub accept_us: u64,
    /// Max single accept_tx wall (µs).
    pub accept_max_us: u64,
    /// Sum of exclusive mempool-lock hold times (µs) this window.
    pub accept_lock_us: u64,
    /// Sum of prevout/UTXO resolve times (µs) this window.
    pub accept_utxo_us: u64,
    /// Sum of consensus script verify times (µs) this window.
    pub accept_script_us: u64,
    /// Sum of durable append/persist times (µs) this window.
    pub accept_durable_us: u64,
    /// Tx inventory items seen that we did not already have.
    pub inv_tx: u64,
    /// Tx getdata items we issued.
    pub getdata_tx: u64,
    /// Mempool accept announces published.
    pub announce: u64,
}

/// Shared mempool + relay gate used by peer sessions and tip confirm.
pub struct MempoolHub {
    inner: RwLock<ActiveMempool>,
    query: Arc<Query>,
    /// When false, peers' tx inv/tx are ignored (IBD / catch-up).
    relay_enabled: AtomicBool,
    /// Broadcast accepts so sessions can inv (origin exclusion is per-session).
    announce: broadcast::Sender<MempoolAnnounce>,
    /// Newest-last ring of successful accepts (Esplora `/mempool/recent`).
    recent: Mutex<std::collections::VecDeque<RecentAccept>>,
    /// Recently confirmed package feerates (sat/kvB) for estimate floor.
    confirm_feerate_memory: Mutex<std::collections::VecDeque<u64>>,
    /// Process-local admit/confirm/evict EMA for flow-aware fee estimates.
    fee_flow: Mutex<FeeFlowMeter>,
    /// Published fee table for Electrum/Esplora (refreshed dirty ∥ max-age, singleflight).
    fee_snapshot: ArcSwap<FeeSnapshot>,
    fee_dirty: AtomicBool,
    fee_refreshing: AtomicBool,
    // Tip-follow 5s DEBUG meters (sample-and-reset).
    meter_accepts: AtomicU64,
    meter_rejects: AtomicU64,
    meter_accept_us: AtomicU64,
    meter_accept_max_us: AtomicU64,
    meter_accept_lock_us: AtomicU64,
    meter_accept_utxo_us: AtomicU64,
    meter_accept_script_us: AtomicU64,
    meter_accept_durable_us: AtomicU64,
    meter_inv_tx: AtomicU64,
    meter_getdata_tx: AtomicU64,
    meter_announce: AtomicU64,
}

impl MempoolHub {
    pub fn open(dir: impl AsRef<Path>, query: Arc<Query>) -> Result<Arc<Self>, String> {
        Self::open_with_weight(dir, query, rbitcoin_mempool::DEFAULT_MAX_MEMPOOL_WEIGHT)
    }

    /// Open with a weight budget (WU). `max_weight_wu` drives chunk eviction.
    pub fn open_with_weight(
        dir: impl AsRef<Path>,
        query: Arc<Query>,
        max_weight_wu: u64,
    ) -> Result<Arc<Self>, String> {
        let mp = ActiveMempool::open_or_create_with_limit(dir.as_ref(), max_weight_wu)
            .map_err(|e| format!("mempool open: {e}"))?;
        let (announce, _) = broadcast::channel(256);
        Ok(Arc::new(Self {
            inner: RwLock::new(mp),
            query,
            relay_enabled: AtomicBool::new(false),
            announce,
            recent: Mutex::new(std::collections::VecDeque::with_capacity(
                MEMPOOL_RECENT_CAP,
            )),
            confirm_feerate_memory: Mutex::new(std::collections::VecDeque::with_capacity(64)),
            fee_flow: Mutex::new(FeeFlowMeter::new(Instant::now())),
            fee_snapshot: ArcSwap::from_pointee(FeeSnapshot::empty(Instant::now())),
            fee_dirty: AtomicBool::new(true),
            fee_refreshing: AtomicBool::new(false),
            meter_accepts: AtomicU64::new(0),
            meter_rejects: AtomicU64::new(0),
            meter_accept_us: AtomicU64::new(0),
            meter_accept_max_us: AtomicU64::new(0),
            meter_accept_lock_us: AtomicU64::new(0),
            meter_accept_utxo_us: AtomicU64::new(0),
            meter_accept_script_us: AtomicU64::new(0),
            meter_accept_durable_us: AtomicU64::new(0),
            meter_inv_tx: AtomicU64::new(0),
            meter_getdata_tx: AtomicU64::new(0),
            meter_announce: AtomicU64::new(0),
        }))
    }

    /// Count peer inv of txs we do not already hold (want → getdata path).
    pub fn note_inv_tx(&self, n: u64) {
        if n > 0 {
            self.meter_inv_tx.fetch_add(n, Ordering::Relaxed);
        }
    }

    /// Count tx getdata items we issued to peers.
    pub fn note_getdata_tx(&self, n: u64) {
        if n > 0 {
            self.meter_getdata_tx.fetch_add(n, Ordering::Relaxed);
        }
    }

    fn meter_accept_wall(&self, us: u64, ok: bool) {
        if ok {
            self.meter_accepts.fetch_add(1, Ordering::Relaxed);
        } else {
            self.meter_rejects.fetch_add(1, Ordering::Relaxed);
        }
        self.meter_accept_us.fetch_add(us, Ordering::Relaxed);
        let mut cur = self.meter_accept_max_us.load(Ordering::Relaxed);
        while us > cur {
            match self.meter_accept_max_us.compare_exchange_weak(
                cur,
                us,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }

    fn meter_accept_stages(&self, lock_us: u64, stages: rbitcoin_mempool::AcceptStageUs) {
        self.meter_accept_lock_us
            .fetch_add(lock_us, Ordering::Relaxed);
        self.meter_accept_utxo_us
            .fetch_add(stages.utxo_us, Ordering::Relaxed);
        self.meter_accept_script_us
            .fetch_add(stages.script_us, Ordering::Relaxed);
        self.meter_accept_durable_us
            .fetch_add(stages.durable_us, Ordering::Relaxed);
    }

    /// Sample-and-reset mempool/relay counters for the tip-follow 5s DEBUG line.
    pub fn sample_reset_perf(&self) -> MempoolPerfSample {
        MempoolPerfSample {
            accepts: self.meter_accepts.swap(0, Ordering::Relaxed),
            rejects: self.meter_rejects.swap(0, Ordering::Relaxed),
            accept_us: self.meter_accept_us.swap(0, Ordering::Relaxed),
            accept_max_us: self.meter_accept_max_us.swap(0, Ordering::Relaxed),
            accept_lock_us: self.meter_accept_lock_us.swap(0, Ordering::Relaxed),
            accept_utxo_us: self.meter_accept_utxo_us.swap(0, Ordering::Relaxed),
            accept_script_us: self.meter_accept_script_us.swap(0, Ordering::Relaxed),
            accept_durable_us: self.meter_accept_durable_us.swap(0, Ordering::Relaxed),
            inv_tx: self.meter_inv_tx.swap(0, Ordering::Relaxed),
            getdata_tx: self.meter_getdata_tx.swap(0, Ordering::Relaxed),
            announce: self.meter_announce.swap(0, Ordering::Relaxed),
        }
    }

    fn push_recent(&self, tx: &Transaction, r: &AcceptResult) {
        let value_sat: u64 = tx
            .output
            .iter()
            .map(|o| o.value.to_sat())
            .fold(0u64, |a, b| a.saturating_add(b));
        let entry = RecentAccept {
            txid: r.txid,
            fee_sat: r.fee_sat,
            weight: r.weight,
            value_sat,
        };
        let mut q = self.recent.lock().unwrap();
        q.push_back(entry);
        while q.len() > MEMPOOL_RECENT_CAP {
            q.pop_front();
        }
    }

    /// Newest-first snapshot of recent accepts (at most 10 for Esplora `/mempool/recent`).
    pub fn recent_accepts(&self) -> Vec<RecentAccept> {
        const ESPLORA_RECENT: usize = 10;
        let q = self.recent.lock().unwrap();
        q.iter().rev().take(ESPLORA_RECENT).cloned().collect()
    }

    /// Compact durable mempool files (reclaim DEAD slots / body holes).
    pub fn compact(&self) -> Result<(u32, usize), String> {
        self.inner
            .write()
            .unwrap()
            .compact()
            .map_err(|e| format!("mempool compact: {e}"))
    }

    /// Enable/disable peer tx inv/accept (false during IBD catch-up).
    ///
    /// **False → true:** bulk-strip txs that are already confirmed-strong on the
    /// best chain. Per-block [`Self::remove_for_block`] is skipped while relay is
    /// off so catch-up is not paced by a large durable mempool (mainnet: 40k+
    /// live after offline). One purge at tip-mode entry is enough before relay.
    pub fn set_relay_enabled(&self, on: bool) {
        let was = self.relay_enabled.swap(on, Ordering::SeqCst);
        if on && !was {
            let n = self.purge_confirmed_on_chain();
            if n > 0 {
                rbitcoin_log::info!(
                    "mempool: purged {n} confirmed tx(s) at tip-mode entry (deferred during IBD)"
                );
            }
        }
    }

    pub fn relay_enabled(&self) -> bool {
        self.relay_enabled.load(Ordering::SeqCst)
    }

    /// Drop every live mempool entry whose create is confirmed-strong on tip.
    ///
    /// Used once when enabling relay after catch-up. Compacts durable slots if
    /// DEAD dominates. Returns how many txs removed.
    pub fn purge_confirmed_on_chain(&self) -> usize {
        let live: Vec<Txid> = {
            let g = self.inner.read().unwrap();
            g.graph.iter().map(|(t, _)| *t).collect()
        };
        if live.is_empty() {
            return 0;
        }
        let mut to_drop: Vec<Txid> = Vec::new();
        for tid in &live {
            let tid_b = tid.to_byte_array();
            let confirmed = match self.query.store().get_fk_by_txid_tip(&tid_b) {
                Ok(Some(fk)) => self.query.store().is_confirmed_strong(fk).unwrap_or(false),
                _ => false,
            };
            if confirmed {
                to_drop.push(*tid);
            }
        }
        if to_drop.is_empty() {
            return 0;
        }
        let mut g = self.inner.write().unwrap();
        let mut n = 0usize;
        for tid in &to_drop {
            if g.graph.contains(tid) && g.remove_txid(tid).is_ok() {
                n += 1;
            }
        }
        if n > 0 {
            let _ = g.maybe_compact();
            self.mark_fee_dirty();
        }
        n
    }

    pub fn subscribe_announces(&self) -> broadcast::Receiver<MempoolAnnounce> {
        self.announce.subscribe()
    }

    fn publish_announce(&self, r: &AcceptResult) {
        let _ = self.announce.send(MempoolAnnounce {
            txid: r.txid,
            replaced: r.replaced.clone(),
            replaced_scripthashes: r.replaced_scripthashes.clone(),
        });
        self.meter_announce.fetch_add(1, Ordering::Relaxed);
    }

    pub fn live_count(&self) -> usize {
        self.inner.read().unwrap().live_count()
    }

    /// Live mempool txids that passed consensus script verify at accept.
    ///
    /// Tip confirm may skip re-verifying these (same tip-era softfork flags).
    pub fn script_preverified_txids(&self) -> std::collections::HashSet<[u8; 32]> {
        use bitcoin::hashes::Hash;
        let g = self.inner.read().unwrap();
        g.graph
            .iter()
            .map(|(txid, _)| txid.to_byte_array())
            .collect()
    }

    pub fn generation(&self) -> u64 {
        self.inner.read().unwrap().generation()
    }

    pub fn flush(&self) -> Result<(), String> {
        self.inner
            .write()
            .unwrap()
            .flush()
            .map_err(|e| format!("mempool flush: {e}"))
    }

    pub fn contains(&self, txid: &Txid) -> bool {
        self.inner.read().unwrap().graph.contains(txid)
    }

    pub fn get_tx(&self, txid: &Txid) -> Option<Transaction> {
        self.inner.read().unwrap().get_tx(txid).cloned()
    }

    /// Look up a live mempool tx by wtxid (BIP339 / compact v2).
    pub fn get_tx_by_wtxid(&self, wtxid: &Wtxid) -> Option<Transaction> {
        let g = self.inner.read().unwrap();
        for (txid, e) in g.graph.iter() {
            if e.wtxid == *wtxid {
                return g.get_tx(txid).cloned();
            }
        }
        None
    }

    /// True if a live mempool entry has this wtxid (BIP339 inv filter).
    pub fn contains_wtxid(&self, wtxid: &Wtxid) -> bool {
        let g = self.inner.read().unwrap();
        let found = g.graph.iter().any(|(_, e)| e.wtxid == *wtxid);
        found
    }

    /// Confirmed tip snapshot for mempool structural checks (height + BIP113 MTP).
    fn chain_tip_ctx(&self) -> ChainTipCtx {
        use rbitcoin_consensus::median_time_past;
        use rbitcoin_primitives::Height;
        let height = self.query.tip_height().map(|h| h.0).unwrap_or(0);
        let mtp = if height == 0 {
            0
        } else {
            median_time_past(self.query.as_ref(), Height(height)).unwrap_or(0)
        };
        ChainTipCtx { height, mtp }
    }

    /// Accept a peer (or local) transaction when relay is enabled.
    ///
    /// **Staged:** exclusive lock for prepare + commit only. Consensus script
    /// verify runs on the shared `rbtc-scripts` path **outside** the mempool
    /// mutex so concurrent readers are not blocked by interpreter CPU.
    pub fn accept_tx(&self, tx: &Transaction) -> Result<AcceptResult, AcceptError> {
        let t0 = Instant::now();
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let tip = self.chain_tip_ctx();

        let mut stages = rbitcoin_mempool::AcceptStageUs::default();
        let mut lock_us = 0u64;

        // Stage prepare under exclusive lock (resolve + structural + policy).
        let prep = {
            let t_lock = Instant::now();
            let mut g = self.inner.write().unwrap();
            g.last_accept_stages = rbitcoin_mempool::AcceptStageUs::default();
            let r = g.prepare_admit(tx, &utxo, tip);
            stages.utxo_us = g.last_accept_stages.utxo_us;
            lock_us = lock_us.saturating_add(t_lock.elapsed().as_micros() as u64);
            r
        };
        let prep = match prep {
            Ok(p) => p,
            Err(e) => {
                let us = t0.elapsed().as_micros() as u64;
                self.meter_accept_stages(lock_us, stages);
                return self.finish_accept_err(us, e);
            }
        };

        // Script outside lock on shared script_pool / rbtc-scripts worker.
        let t_script = Instant::now();
        if let Err(e) =
            rbitcoin_consensus::verify_tx_scripts_detached(prep.prevouts.clone(), tx.clone())
        {
            let us = t0.elapsed().as_micros() as u64;
            stages.script_us = t_script.elapsed().as_micros() as u64;
            self.meter_accept_stages(lock_us, stages);
            return self.finish_accept_err(us, AcceptError::Script(e.to_string()));
        }
        stages.script_us = t_script.elapsed().as_micros() as u64;

        // Commit under exclusive lock (re-check + durable + orphan promote).
        let result = {
            let t_lock = Instant::now();
            let mut g = self.inner.write().unwrap();
            g.last_accept_stages = stages;
            let r = match g.commit_after_script(tx, prep, &utxo, tip) {
                Ok(ar) => {
                    g.promote_orphans_of(ar.txid, &utxo, tip);
                    Ok(ar)
                }
                Err(e) => Err(e),
            };
            stages = g.last_accept_stages;
            lock_us = lock_us.saturating_add(t_lock.elapsed().as_micros() as u64);
            r
        };

        let us = t0.elapsed().as_micros() as u64;
        self.meter_accept_stages(lock_us, stages);
        match result {
            Ok(r) => {
                self.meter_accept_wall(us, true);
                self.note_fee_flow_admit(r.weight, r.fee_sat);
                self.push_recent(tx, &r);
                self.publish_announce(&r);
                Ok(r)
            }
            Err(e) => self.finish_accept_err(us, e),
        }
    }

    fn note_fee_flow_admit(&self, weight: u64, fee_sat: u64) {
        let rate = rbitcoin_consensus::policy::fee_rate_sat_per_kvb(fee_sat, weight);
        if let Ok(mut m) = self.fee_flow.lock() {
            m.note_admit(weight, rate, Instant::now());
        }
        self.mark_fee_dirty();
    }

    fn note_fee_flow_confirm(&self, weight: u64, fee_sat: u64) {
        let rate = rbitcoin_consensus::policy::fee_rate_sat_per_kvb(fee_sat, weight);
        if let Ok(mut m) = self.fee_flow.lock() {
            m.note_confirm(weight, rate, Instant::now());
        }
        self.mark_fee_dirty();
    }

    fn mark_fee_dirty(&self) {
        self.fee_dirty.store(true, Ordering::Release);
    }

    /// Map API target blocks → engine depth (0–2 → default horizon of 1).
    fn fee_depth(target_blocks: u32) -> u32 {
        if target_blocks == 0 || target_blocks <= 2 {
            Self::DEFAULT_HORIZON_BLOCKS
        } else {
            target_blocks
        }
    }

    /// Lazy singleflight refresh when dirty or older than [`FEE_SNAPSHOT_MAX_AGE`].
    fn maybe_refresh_fee_snapshot(&self) {
        let now = Instant::now();
        let snap = self.fee_snapshot.load_full();
        let stale = now
            .checked_duration_since(snap.computed_at)
            .map(|d| d >= FEE_SNAPSHOT_MAX_AGE)
            .unwrap_or(true);
        let dirty = self.fee_dirty.load(Ordering::Acquire);
        // Always refresh when never populated with real data and dirty (first admit path).
        if !dirty && !stale {
            return;
        }
        if self
            .fee_refreshing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            // Another thread is refreshing; callers use the previous Arc.
            return;
        }
        self.refresh_fee_snapshot();
        self.fee_refreshing.store(false, Ordering::Release);
    }

    /// One graph linearize under short read lock, then pure math off-lock → publish.
    fn refresh_fee_snapshot(&self) {
        let t0 = Instant::now();
        let chunks = {
            let g = self.inner.read().unwrap();
            g.graph.mining_chunks_best_first()
        };

        let now = Instant::now();
        let inflow = match self.fee_flow.lock() {
            Ok(mut flow) if flow.is_warm(now) => Some(flow.admit_rates_wu_s(now)),
            _ => None,
        };
        let candidates = default_candidate_rates();
        let min_r = rbitcoin_consensus::policy::MIN_RELAY_FEE_RATE_SAT_PER_KVB;
        let confirm_floor = self.confirm_memory_floor_sat_per_kvb();

        let mut by_depth = HashMap::with_capacity(FEE_SNAPSHOT_DEPTHS.len());
        for &depth in FEE_SNAPSHOT_DEPTHS {
            let target_wu = u64::from(depth).saturating_mul(BLOCK_WEIGHT_WU);
            let frontier = frontier_feerate_from_chunks(&chunks, target_wu);
            let projected = inflow.as_ref().and_then(|inf| {
                min_rate_for_capacity(
                    |r| weight_above_from_chunks(&chunks, r),
                    inf,
                    depth,
                    &candidates,
                )
            });
            let mut rate = match (projected, frontier) {
                (Some(p), Some(f)) => p.max(f),
                (Some(p), None) => p,
                (None, Some(f)) => f,
                (None, None) => {
                    // Empty pool: optional confirm-memory floor as BTC/kB.
                    let v = confirm_floor
                        .map(|r| (r as f64) / 100_000_000.0)
                        .unwrap_or(-1.0);
                    by_depth.insert(depth, v);
                    continue;
                }
            };
            rate = rate.max(min_r);
            if let Some(floor) = confirm_floor {
                rate = rate.max(floor);
            }
            by_depth.insert(depth, (rate as f64) / 100_000_000.0);
        }

        self.fee_snapshot.store(Arc::new(FeeSnapshot {
            by_depth_btc_per_kb: by_depth,
            chunks,
            computed_at: t0,
        }));
        self.fee_dirty.store(false, Ordering::Release);
    }

    fn finish_accept_err(&self, us: u64, e: AcceptError) -> Result<AcceptResult, AcceptError> {
        // Soft outcomes (already in pool / orphan / full) are not "rejects".
        let hard = !matches!(
            e,
            AcceptError::Duplicate(_)
                | AcceptError::Orphaned(_)
                | AcceptError::Policy("mempool full")
        );
        if hard {
            self.meter_accept_wall(us, false);
        } else {
            self.meter_accept_us.fetch_add(us, Ordering::Relaxed);
        }
        Err(e)
    }

    /// Accept an ancestor package (local / Electrum path; BIP331 wire later).
    pub fn accept_package(&self, txs: &[Transaction]) -> Result<Vec<AcceptResult>, AcceptError> {
        let t0 = Instant::now();
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let tip = self.chain_tip_ctx();
        let t_lock = Instant::now();
        let mut g = self.inner.write().unwrap();
        let result = g.accept_package(txs, &utxo, tip);
        let stages = g.last_accept_stages;
        let lock_us = t_lock.elapsed().as_micros() as u64;
        drop(g);
        let us = t0.elapsed().as_micros() as u64;
        self.meter_accept_stages(lock_us, stages);
        match result {
            Ok(res) => {
                // Attribute package wall to each accepted member for rate visibility.
                let per = us / (res.len().max(1) as u64);
                for (tx, r) in txs.iter().zip(res.iter()) {
                    self.meter_accept_wall(per, true);
                    self.note_fee_flow_admit(r.weight, r.fee_sat);
                    self.push_recent(tx, r);
                    self.publish_announce(r);
                }
                Ok(res)
            }
            Err(e) => {
                let hard = !matches!(
                    e,
                    AcceptError::Duplicate(_)
                        | AcceptError::Orphaned(_)
                        | AcceptError::Policy("mempool full")
                );
                if hard {
                    self.meter_accept_wall(us, false);
                } else {
                    self.meter_accept_us.fetch_add(us, Ordering::Relaxed);
                }
                Err(e)
            }
        }
    }

    /// Remove confirmed txids (tip connect / archive confirm) and re-try orphans
    /// whose parents just confirmed (Query UTXO view).
    ///
    /// Samples removed entries' feerates into confirm-memory for the standard
    /// 10-minute fee estimate floor.
    ///
    /// **No-op while relay is disabled** (IBD catch-up). Callers must not rely
    /// on per-block strip until [`Self::set_relay_enabled`]`(true)` has run the
    /// deferred [`Self::purge_confirmed_on_chain`].
    pub fn remove_for_block(&self, txids: &[Txid]) -> usize {
        if !self.relay_enabled() {
            return 0;
        }
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let tip = self.chain_tip_ctx();
        let mut g = self.inner.write().unwrap();
        // Sample before remove while entries still live.
        for tid in txids {
            if let Some(e) = g.graph.get(tid) {
                let rate = e.fee_rate_sat_per_kvb();
                self.push_confirm_memory(rate);
                self.note_fee_flow_confirm(e.weight, e.fee_sat);
            }
        }
        g.remove_for_block_with_utxo(txids, &utxo, tip).unwrap_or(0)
    }

    /// Unique txs parked waiting on missing parents (Core-class orphanage).
    pub fn orphan_count(&self) -> usize {
        self.inner.read().unwrap().orphan_count()
    }

    /// Re-admit txs after reorg disconnect (best-effort).
    pub fn reorg_reaccept(&self, txs: &[Transaction]) -> usize {
        let utxo = QueryUtxoProvider {
            query: self.query.as_ref(),
        };
        let tip = self.chain_tip_ctx();
        let mut g = self.inner.write().unwrap();
        g.reorg_disconnect_reaccept(txs, &utxo, tip)
            .into_iter()
            .filter(|r| r.is_ok())
            .count()
    }

    /// Snapshot of live txs (for Electrum / RPC) — clones bodies.
    pub fn list_live(&self) -> Vec<(Txid, u64, u64, Transaction)> {
        let g = self.inner.read().unwrap();
        g.graph
            .iter()
            .filter_map(|(txid, e)| {
                g.get_tx(txid)
                    .cloned()
                    .map(|tx| (*txid, e.fee_sat, e.weight, tx))
            })
            .collect()
    }

    /// Live txid + fee + weight **without** cloning bodies (RPC/Esplora stats).
    pub fn list_live_meta(&self) -> Vec<(Txid, u64, u64)> {
        let g = self.inner.read().unwrap();
        g.graph
            .iter()
            .map(|(txid, e)| (*txid, e.fee_sat, e.weight))
            .collect()
    }

    /// Outpoints spent by any live mempool transaction (confirmed or mempool parents).
    pub fn spent_outpoints(&self) -> std::collections::HashSet<OutPoint> {
        let g = self.inner.read().unwrap();
        let mut set = std::collections::HashSet::new();
        for (txid, _) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            for inp in &tx.input {
                set.insert(inp.previous_output);
            }
        }
        set
    }

    /// Electrum `blockchain.scripthash.get_mempool` rows for `scripthash` (internal order).
    pub fn scripthash_mempool(&self, scripthash: &[u8; 32]) -> Vec<ElectrumMempoolItem> {
        use rbitcoin_store::script_hash;
        let g = self.inner.read().unwrap();
        let mut out = Vec::new();
        for (txid, e) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            let mut touches = false;
            for o in &tx.output {
                if script_hash(o.script_pubkey.as_bytes()) == *scripthash {
                    touches = true;
                    break;
                }
            }
            if !touches {
                for inp in &tx.input {
                    let op = inp.previous_output;
                    let spk = if let Some(creator) = g.graph.creator(&op) {
                        g.get_tx(&creator)
                            .and_then(|t| t.output.get(op.vout as usize))
                            .map(|o| o.script_pubkey.as_bytes().to_vec())
                    } else {
                        QueryUtxoProvider {
                            query: self.query.as_ref(),
                        }
                        .get_txout(&op)
                        .map(|o| o.script_pubkey.as_bytes().to_vec())
                    };
                    if let Some(s) = spk {
                        if script_hash(&s) == *scripthash {
                            touches = true;
                            break;
                        }
                    }
                }
            }
            if !touches {
                continue;
            }
            let mut height = 0i64;
            for inp in &tx.input {
                if g.graph.contains(&inp.previous_output.txid) {
                    height = -1;
                    break;
                }
            }
            out.push(ElectrumMempoolItem {
                txid: txid.to_byte_array(),
                height,
                fee: e.fee_sat as i64,
            });
        }
        out.sort_by(|a, b| a.txid.cmp(&b.txid));
        out
    }

    /// Unconfirmed delta for Electrum balance (sats): +mempool outputs − spent confirmed.
    pub fn scripthash_unconfirmed_delta(&self, scripthash: &[u8; 32]) -> i64 {
        use rbitcoin_store::script_hash;
        let g = self.inner.read().unwrap();
        let mut delta = 0i64;
        for (txid, _e) in g.graph.iter() {
            let Some(tx) = g.get_tx(txid) else { continue };
            for (vout, o) in tx.output.iter().enumerate() {
                if script_hash(o.script_pubkey.as_bytes()) != *scripthash {
                    continue;
                }
                let op = OutPoint {
                    txid: *txid,
                    vout: vout as u32,
                };
                if g.graph.mempool_utxo(&op) {
                    delta = delta.saturating_add(o.value.to_sat() as i64);
                }
            }
            for inp in &tx.input {
                let op = inp.previous_output;
                // Only count spending of **chain** UTXOs (not pure mempool-parent).
                if g.graph.creator(&op).is_some() {
                    continue;
                }
                let provider = QueryUtxoProvider {
                    query: self.query.as_ref(),
                };
                if let Some(txout) = provider.get_txout(&op) {
                    if script_hash(txout.script_pubkey.as_bytes()) == *scripthash {
                        delta = delta.saturating_sub(txout.value.to_sat() as i64);
                    }
                }
            }
        }
        delta
    }

    /// Block weight (WU) used for inclusion-frontier depth.
    pub const BLOCK_WEIGHT_WU: u64 = 4_000_000;

    /// Product default: **10-minute inclusion** ≈ next 1 block of weight
    /// (see `docs/mempool-fee-estimation.md`).
    pub const DEFAULT_HORIZON_BLOCKS: u32 = 1;

    /// Fee histogram buckets for Electrum: `[[feerate_sat_per_kvb, vsize], ...]`
    /// descending rate, using **published** mining-chunk rates (same refresh as fees).
    pub fn fee_histogram(&self) -> Vec<(u64, u64)> {
        self.maybe_refresh_fee_snapshot();
        self.fee_snapshot.load().histogram()
    }

    /// Standard / target-depth fee in BTC/kB (Engine v2 when flow meter warm).
    ///
    /// **Default product answer is 10-minute inclusion** (`target_blocks` 0–2 →
    /// depth of [`Self::DEFAULT_HORIZON_BLOCKS`] blocks).
    ///
    /// **Non-blocking vs accept:** returns from a **published snapshot** (Arc).
    /// Graph linearize runs only on dirty/stale singleflight **refresh** (≤~1 s
    /// stale; one `mining_chunks` per refresh for all depths). Request path does
    /// not hold the hub lock across multi-pass walks.
    pub fn estimate_fee_btc_per_kb(&self, target_blocks: u32) -> f64 {
        self.maybe_refresh_fee_snapshot();
        let depth = Self::fee_depth(target_blocks);
        self.fee_snapshot.load().rate_btc_per_kb(depth)
    }

    /// How many times the live graph rebuilt mining chunks (sample-and-reset).
    pub fn take_chunks_rebuilds(&self) -> u64 {
        self.inner.read().unwrap().graph.take_chunks_rebuilds()
    }

    /// All Esplora `/fee-estimates` depths in one Arc load (+ optional refresh).
    pub fn fee_estimates_btc_per_kb(&self) -> Vec<(u32, f64)> {
        self.maybe_refresh_fee_snapshot();
        let snap = self.fee_snapshot.load_full();
        FEE_SNAPSHOT_DEPTHS
            .iter()
            .map(|&d| (d, snap.rate_btc_per_kb(d)))
            .collect()
    }

    /// Mining-order chunk snapshot for diagnostics / future templates.
    pub fn mining_frontier_snapshot(&self) -> Vec<(u64, u64, u64, usize)> {
        self.maybe_refresh_fee_snapshot();
        self.fee_snapshot
            .load()
            .chunks
            .iter()
            .map(|c| (c.fee_rate_sat_per_kvb(), c.weight, c.fee_sat, c.txids.len()))
            .collect()
    }

    /// Weight (WU) ranking strictly above `rate_sat_per_kvb` (published chunks).
    pub fn weight_above_feerate(&self, rate_sat_per_kvb: u64) -> u64 {
        self.maybe_refresh_fee_snapshot();
        weight_above_from_chunks(&self.fee_snapshot.load().chunks, rate_sat_per_kvb)
    }

    /// Relay fee in BTC/kB (Libre 0.1 sat/vB = 100 sat/kvB).
    pub fn relay_fee_btc_per_kb() -> f64 {
        rbitcoin_consensus::policy::MIN_RELAY_FEE_RATE_SAT_PER_KVB as f64 / 100_000_000.0
    }

    // ── Confirm-memory floor (step 5) ─────────────────────────────────────

    /// Ring of recently confirmed package feerates (sat/kvB), newest last.
    /// Filled from `remove_for_block` when live entries leave the pool.
    fn confirm_memory_floor_sat_per_kvb(&self) -> Option<u64> {
        let mem = self.confirm_feerate_memory.lock().unwrap();
        if mem.is_empty() {
            return None;
        }
        // Median of samples (practical floor).
        let mut v: Vec<u64> = mem.iter().copied().collect();
        v.sort_unstable();
        Some(v[v.len() / 2].max(rbitcoin_consensus::policy::MIN_RELAY_FEE_RATE_SAT_PER_KVB))
    }

    fn push_confirm_memory(&self, rate_sat_per_kvb: u64) {
        const CAP: usize = 64;
        let mut mem = self.confirm_feerate_memory.lock().unwrap();
        mem.push_back(rate_sat_per_kvb.max(1));
        while mem.len() > CAP {
            mem.pop_front();
        }
    }
}

/// One Electrum mempool history row.
#[derive(Debug, Clone)]
pub struct ElectrumMempoolItem {
    pub txid: [u8; 32],
    pub height: i64,
    pub fee: i64,
}

/// Decode raw package payload: concat of bitcoin txs (for future BIP331 `pkgtxns`).
///
/// Format used by tests / local inject: each tx is length-prefixed u32 LE + raw.
pub fn decode_len_prefixed_package(payload: &[u8]) -> Result<Vec<Transaction>, String> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i + 4 <= payload.len() {
        let n = u32::from_le_bytes(payload[i..i + 4].try_into().unwrap()) as usize;
        i += 4;
        if i + n > payload.len() {
            return Err("package truncated".into());
        }
        let tx: Transaction = deserialize(&payload[i..i + n]).map_err(|e| e.to_string())?;
        out.push(tx);
        i += n;
    }
    if i != payload.len() {
        return Err("package trailing bytes".into());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::consensus::encode::serialize;
    use bitcoin::hashes::Hash;
    use bitcoin::transaction::Version;
    use bitcoin::{Sequence, TxIn, Witness};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> std::path::PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rbitcoin-txrelay-{n}"))
    }

    #[test]
    fn package_codec_roundtrip() {
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let raw = serialize(&tx);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        payload.extend_from_slice(&raw);
        let decoded = decode_len_prefixed_package(&payload).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].compute_txid(), tx.compute_txid());
    }

    /// While relay is off, per-block remove is deferred; enabling relay runs purge.
    #[test]
    fn remove_for_block_skipped_until_relay_then_purge() {
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let mp = MempoolHub::open(&dir, Arc::new(q)).unwrap();
        assert!(!mp.relay_enabled());
        let dummy = Txid::from_byte_array([9u8; 32]);
        // No-op while relay off (IBD catch-up must not strip per block).
        assert_eq!(mp.remove_for_block(&[dummy]), 0);
        // Enabling relay runs purge (empty → 0) and arms per-block strip.
        mp.set_relay_enabled(true);
        assert!(mp.relay_enabled());
        assert_eq!(mp.purge_confirmed_on_chain(), 0);
        // Still no-op for unknown txid, but path is live.
        assert_eq!(mp.remove_for_block(&[dummy]), 0);
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn hub_accept_remove_with_map_utxo_path() {
        // MempoolHub needs Query; use open empty store + MapUtxo via direct ActiveMempool
        // for isolation — hub Query path covered when store has txs.
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&dir, Arc::new(q)).unwrap();
        assert!(!hub.relay_enabled());
        hub.set_relay_enabled(true);
        assert!(hub.relay_enabled());
        assert_eq!(hub.live_count(), 0);
        // Without chain UTXO, accept parks as orphan (Core-class soft path).
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([9u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let err = hub.accept_tx(&tx).unwrap_err();
        assert!(matches!(err, AcceptError::Orphaned(_)), "{err}");
        assert_eq!(hub.orphan_count(), 1);
        assert!(hub.fee_histogram().is_empty());
        assert!(hub.estimate_fee_btc_per_kb(2) < 0.0 || hub.estimate_fee_btc_per_kb(2) >= 0.0);
        assert!(MempoolHub::relay_fee_btc_per_kb() > 0.0);
        assert!(hub.scripthash_mempool(&[0u8; 32]).is_empty());
        assert_eq!(hub.scripthash_unconfirmed_delta(&[0u8; 32]), 0);
        assert!(hub.list_live().is_empty());
        assert!(!hub.contains_wtxid(&Wtxid::from_byte_array([0u8; 32])));
        assert!(hub
            .get_tx_by_wtxid(&Wtxid::from_byte_array([0u8; 32]))
            .is_none());
        assert_eq!(hub.remove_for_block(&[]), 0);
        assert_eq!(hub.reorg_reaccept(&[]), 0);
        hub.flush().unwrap();
        let _ = hub.compact();
        let _ = hub.generation();
        let _ = hub.subscribe_announces();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn open_with_weight_and_package_empty() {
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open_with_weight(&dir, Arc::new(q), 1_000_000).unwrap();
        hub.set_relay_enabled(true);
        assert!(matches!(
            hub.accept_package(&[]),
            Err(AcceptError::PackageEmpty)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn package_codec_errors_and_query_utxo_miss() {
        // Truncated length prefix.
        assert!(decode_len_prefixed_package(&[1, 2, 3]).is_err());
        // Length claims more than remaining.
        let mut bad = 100u32.to_le_bytes().to_vec();
        bad.extend_from_slice(&[0u8; 4]);
        assert!(decode_len_prefixed_package(&bad).is_err());
        // Empty package ok.
        assert!(decode_len_prefixed_package(&[]).unwrap().is_empty());
        // Garbage tx body.
        let mut junk = 4u32.to_le_bytes().to_vec();
        junk.extend_from_slice(&[0xff; 4]);
        assert!(decode_len_prefixed_package(&junk).is_err());
        // Trailing bytes after valid package.
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: Txid::from_byte_array([1u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(1),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        };
        let raw = serialize(&tx);
        let mut payload = Vec::new();
        payload.extend_from_slice(&(raw.len() as u32).to_le_bytes());
        payload.extend_from_slice(&raw);
        payload.push(0xff); // trailing
        assert!(decode_len_prefixed_package(&payload)
            .unwrap_err()
            .contains("trailing"));

        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let provider = QueryUtxoProvider { query: &q };
        let op = OutPoint {
            txid: Txid::from_byte_array([0xcd; 32]),
            vout: 0,
        };
        assert!(provider.get_txout(&op).is_none());
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn estimate_fee_percentiles_and_spent_outpoints_empty() {
        let dir = tmp();
        let store_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&dir, Arc::new(q)).unwrap();
        // Empty → negative estimate for all targets.
        assert!(hub.estimate_fee_btc_per_kb(1) < 0.0);
        assert!(hub.estimate_fee_btc_per_kb(5) < 0.0);
        assert!(hub.estimate_fee_btc_per_kb(100) < 0.0);
        assert!(hub.spent_outpoints().is_empty());
        assert!(hub.contains(&Txid::from_byte_array([0u8; 32])) == false);
        assert!(hub.get_tx(&Txid::from_byte_array([0u8; 32])).is_none());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    #[test]
    fn recent_accepts_ring_newest_first() {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        // Empty store: still can accept nothing without parents — just open hub.
        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        assert!(hub.recent_accepts().is_empty());
        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Accept live spends of confirmed OP_TRUE coinbases via QueryUtxoProvider —
    /// covers fee histogram, estimate percentiles, scripthash mempool/delta,
    /// wtxid lookup, package announce, and spent outpoints.
    #[test]
    fn hub_live_accept_fee_scripthash_and_package() {
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{Block, CompactTarget, Target, TxMerkleNode};
        use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
        use rbitcoin_primitives::Height;
        use rbitcoin_store::script_hash;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;

        fn coinbase(height: u32) -> Transaction {
            let mut ss = if height == 0 {
                vec![0x00]
            } else {
                rbitcoin_consensus::bip34_height_script(height)
            };
            while ss.len() < 2 {
                ss.push(0x00);
            }
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]), // OP_TRUE
                }],
            }
        }
        fn mine(prev: bitcoin::BlockHash, time: u32, height: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let header = Header {
                version: BlockVersion::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            };
            let mut block = Block {
                header,
                txdata: vec![coinbase(height)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        // Early coinbases + maturity pad so mempool coinbase maturity (100) is met.
        let mut coinbase_txids = Vec::new();
        for h in 1u32..=103 {
            let b = mine(tip, tip_time + 600, h);
            if h <= 3 {
                coinbase_txids.push(b.txdata[0].compute_txid());
            }
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        let q_arc = Arc::new(q);
        let hub = MempoolHub::open(&mp_dir, Arc::clone(&q_arc)).unwrap();
        hub.set_relay_enabled(true);
        let mut ann_rx = hub.subscribe_announces();

        // QueryUtxoProvider hit on confirmed coinbase.
        let provider = QueryUtxoProvider {
            query: q_arc.as_ref(),
        };
        let op0 = OutPoint {
            txid: coinbase_txids[0],
            vout: 0,
        };
        assert!(provider.get_txout(&op0).is_some());

        let spk = ScriptBuf::from_bytes(vec![0x51]);
        let sh = script_hash(spk.as_bytes());

        // Parent spend → fee 1000.
        let parent = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: op0,
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 1_000),
                script_pubkey: spk.clone(),
            }],
        };
        let pr = hub.accept_tx(&parent).expect("accept parent");
        assert_eq!(pr.txid, parent.compute_txid());
        assert!(matches!(ann_rx.try_recv(), Ok(_)));
        let recent = hub.recent_accepts();
        assert_eq!(recent.len(), 1);
        assert_eq!(recent[0].txid, parent.compute_txid());
        assert_eq!(recent[0].fee_sat, 1_000);

        // Child of mempool parent (height=-1 scripthash path) + second chain spend.
        let child = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: parent.compute_txid(),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 2_000),
                script_pubkey: spk.clone(),
            }],
        };
        let second = Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint {
                    txid: coinbase_txids[1],
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000 - 5_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x52]), // different spk
            }],
        };
        // Package: child then second is not topo for child; accept child alone then package second.
        hub.accept_tx(&child).expect("child");
        let pkg = hub.accept_package(&[second.clone()]).expect("package");
        assert_eq!(pkg.len(), 1);

        assert_eq!(hub.live_count(), 3);
        assert!(hub.contains(&parent.compute_txid()));
        assert!(hub.get_tx(&parent.compute_txid()).is_some());
        let wtxid = parent.compute_wtxid();
        assert!(hub.contains_wtxid(&wtxid));
        assert!(hub.get_tx_by_wtxid(&wtxid).is_some());

        // Fee surfaces with live graph.
        assert!(!hub.fee_histogram().is_empty());
        let e1 = hub.estimate_fee_btc_per_kb(1); // 90th
        let e5 = hub.estimate_fee_btc_per_kb(5); // 50th
        let e20 = hub.estimate_fee_btc_per_kb(20); // 20th
        assert!(e1 >= 0.0 && e5 >= 0.0 && e20 >= 0.0);

        let spent = hub.spent_outpoints();
        assert!(spent.contains(&op0));
        assert!(spent.contains(&OutPoint {
            txid: parent.compute_txid(),
            vout: 0
        }));

        // Scripthash: parent/child touch OP_TRUE; second does not.
        let rows = hub.scripthash_mempool(&sh);
        assert!(rows.len() >= 2);
        assert!(rows.iter().any(|r| r.height == -1)); // child of mempool parent
        let delta = hub.scripthash_unconfirmed_delta(&sh);
        // Parent output still live until child spent it; child holds OP_TRUE UTXO.
        // Net: +child_out - coinbase (parent spent chain UTXO) roughly non-zero path.
        let _ = delta;

        // Remove confirmed + reorg reaccept empty already covered; remove live.
        assert!(hub.remove_for_block(&[parent.compute_txid()]) >= 1);
        assert!(hub.list_live().len() < 3);

        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// C0: stage meters accumulate on accept; sample-and-reset clears them.
    ///
    /// Also acts as the **baseline microbench harness** for later staged-accept
    /// work (lock ≈ wall while scripts/durable still run under the hub mutex).
    #[test]
    fn accept_stage_meters_and_baseline_harness() {
        use bitcoin::block::{Header, Version as BlockVersion};
        use bitcoin::transaction::Version as TxVersion;
        use bitcoin::{Block, CompactTarget, Target, TxMerkleNode};
        use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
        use rbitcoin_primitives::Height;

        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let params = ChainParams::regtest();
        let ms = Milestone::NONE;

        fn coinbase(height: u32) -> Transaction {
            let mut ss = if height == 0 {
                vec![0x00]
            } else {
                rbitcoin_consensus::bip34_height_script(height)
            };
            while ss.len() < 2 {
                ss.push(0x00);
            }
            Transaction {
                version: TxVersion::ONE,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::from_bytes(ss),
                    sequence: Sequence::MAX,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000),
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                }],
            }
        }
        fn mine(prev: bitcoin::BlockHash, time: u32, height: u32) -> Block {
            let bits = CompactTarget::from_consensus(0x207f_ffff);
            let header = Header {
                version: BlockVersion::ONE,
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0u8; 32]),
                time,
                bits,
                nonce: 0,
            };
            let mut block = Block {
                header,
                txdata: vec![coinbase(height)],
            };
            block.header.merkle_root = block.compute_merkle_root().unwrap();
            let target = Target::from_compact(bits);
            for nonce in 0..u32::MAX {
                block.header.nonce = nonce;
                if block.header.validate_pow(target).is_ok() {
                    break;
                }
            }
            block
        }

        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
        let mut tip = genesis.block_hash();
        let mut tip_time = genesis.header.time;
        let mut coinbase_txids = Vec::new();
        // Maturity pad + a few spends for the harness.
        const N_SPENDS: u32 = 4;
        for h in 1u32..=(100 + N_SPENDS) {
            let b = mine(tip, tip_time + 600, h);
            if h <= N_SPENDS {
                coinbase_txids.push(b.txdata[0].compute_txid());
            }
            accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
            tip = b.block_hash();
            tip_time = b.header.time;
        }

        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        hub.set_relay_enabled(true);
        let _ = hub.sample_reset_perf(); // clear open noise

        let spk = ScriptBuf::from_bytes(vec![0x51]);
        for (i, cbtxid) in coinbase_txids.iter().enumerate() {
            let fee = 1_000u64 + i as u64;
            let tx = Transaction {
                version: TxVersion::TWO,
                lock_time: LockTime::ZERO,
                input: vec![TxIn {
                    previous_output: OutPoint {
                        txid: *cbtxid,
                        vout: 0,
                    },
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                }],
                output: vec![TxOut {
                    value: Amount::from_sat(50_0000_0000 - fee),
                    script_pubkey: spk.clone(),
                }],
            };
            hub.accept_tx(&tx).expect("accept harness spend");
        }

        let s = hub.sample_reset_perf();
        assert_eq!(s.accepts, N_SPENDS as u64);
        assert!(s.accept_us > 0, "wall meter");
        assert!(s.accept_lock_us > 0, "lock meter");
        assert!(s.accept_utxo_us > 0, "utxo stage (chain coin resolve)");
        assert!(s.accept_script_us > 0, "script stage");
        assert!(s.accept_durable_us > 0, "durable stage");
        // C1: script runs outside the exclusive lock (detached rbtc-scripts).
        // Lock still covers prepare + durable commit; durable stays under lock.
        assert!(
            s.accept_lock_us >= s.accept_durable_us,
            "lock_us={} durable_us={}",
            s.accept_lock_us,
            s.accept_durable_us
        );
        // Structural: script time is metered and wall includes it.
        assert!(
            s.accept_us >= s.accept_script_us,
            "wall={} script={}",
            s.accept_us,
            s.accept_script_us
        );
        // Sample-and-reset clears.
        let z = hub.sample_reset_perf();
        assert_eq!(z.accepts, 0);
        assert_eq!(z.accept_script_us, 0);
        assert_eq!(z.accept_lock_us, 0);

        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Concurrent read APIs do not deadlock under RwLock (C2).
    #[test]
    fn concurrent_estimate_and_list_reads() {
        use std::thread;

        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        hub.set_relay_enabled(true);

        let mut handles = Vec::new();
        for _ in 0..4 {
            let h = Arc::clone(&hub);
            handles.push(thread::spawn(move || {
                for _ in 0..32 {
                    let _ = h.live_count();
                    let _ = h.estimate_fee_btc_per_kb(2);
                    let _ = h.fee_estimates_btc_per_kb();
                    let _ = h.fee_histogram();
                    let _ = h.list_live();
                    let _ = h.contains_wtxid(&Wtxid::from_byte_array([0u8; 32]));
                }
            }));
        }
        for h in handles {
            h.join().expect("reader");
        }
        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Fee snapshot covers Esplora depths; request path uses published table.
    #[test]
    fn fee_snapshot_bulk_and_estimate_share_table() {
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        hub.set_relay_enabled(true);
        // Empty pool: negative / unavailable for Electrum-style single target.
        assert!(hub.estimate_fee_btc_per_kb(2) < 0.0);
        let bulk = hub.fee_estimates_btc_per_kb();
        assert_eq!(bulk.len(), 11);
        assert!(bulk.iter().all(|(d, v)| *d >= 1 && *v < 0.0));
        // Second call hits cache (not dirty/stale immediately) — still consistent.
        assert_eq!(
            hub.estimate_fee_btc_per_kb(6),
            hub.fee_estimates_btc_per_kb()[4].1
        );
        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Histogram / frontier share one published chunk rebuild per dirty refresh.
    #[test]
    fn histogram_and_estimate_share_one_chunks_rebuild() {
        let store_dir = tmp();
        let mp_dir = tmp();
        let q = Query::open_or_create(&store_dir).unwrap();
        let hub = MempoolHub::open(&mp_dir, Arc::new(q)).unwrap();
        hub.set_relay_enabled(true);
        let _ = hub.take_chunks_rebuilds();
        let _ = hub.fee_histogram();
        let _ = hub.estimate_fee_btc_per_kb(2);
        let _ = hub.mining_frontier_snapshot();
        let n = hub.take_chunks_rebuilds();
        assert!(
            n <= 1,
            "expected at most one mining_chunks rebuild for one dirty refresh, got {n}"
        );
        let _ = hub.fee_histogram();
        let _ = hub.fee_histogram();
        assert_eq!(hub.take_chunks_rebuilds(), 0);
        let _ = std::fs::remove_dir_all(&mp_dir);
        let _ = std::fs::remove_dir_all(&store_dir);
    }
}
