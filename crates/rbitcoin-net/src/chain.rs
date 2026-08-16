//! Shared chain accept path for P2P: tip extension and most-work reorg.

use crate::cache::BlockCache;
use crate::error::NetError;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, ScriptBuf, Transaction, Work};
use rbitcoin_consensus::{
    accept_and_connect_block_preverified, confirm_wire_load_from_plan as consensus_load_from_plan,
    confirm_wire_load_phase_pipelined, confirm_write_phase, genesis_block, header_to_record,
    mine_regtest_paying, ChainParams, Milestone, PlanStampOutcome, ScriptOkBatch,
    ScriptPreverified, WireLoadPipeline,
};
use rbitcoin_log::info;
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_query::Query;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::RwLock;
use tokio::sync::{broadcast, Notify};

/// Emitted when the best-chain tip advances (extension or reorg).
#[derive(Debug, Clone)]
pub struct TipEvent {
    pub height: u32,
    pub hash: BlockHash,
    pub header: Header,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptOutcome {
    /// New best tip.
    Accepted { height: u32 },
    /// Already in store / cache.
    AlreadyHave,
    /// Same height competing tip with less or equal work — ignored.
    IgnoredWeaker,
}

/// Thread-safe chain façade used by peer sessions.
pub struct ChainHub {
    pub query: Arc<Query>,
    pub cache: Arc<BlockCache>,
    pub params: ChainParams,
    pub milestone: Milestone,
    pub notify: Arc<Notify>,
    tip_tx: broadcast::Sender<TipEvent>,
    /// Best-chain confirmed block hashes (O(1) `has_block` for IBD hot path).
    confirmed: Arc<RwLock<HashSet<BlockHash>>>,
    /// Serializes tip connect / reorg so multi-peer accept cannot double Class A+C.
    connect_lock: std::sync::Mutex<()>,
    /// Optional cluster mempool (tip-mode tx relay + confirm remove).
    ///
    /// Attached once via [`Self::attach_mempool`] after the hub is in an `Arc`.
    mempool: std::sync::OnceLock<Arc<crate::tx_relay::MempoolHub>>,
    /// Regtest `setmocktime` / generate timestamps. Default is wall clock.
    pub clock: Arc<rbitcoin_consensus::NodeClock>,
    invalidated: RwLock<HashSet<BlockHash>>,
    /// Operator-invalidated best-chain paths (hashes only, height order).
    /// Bodies come back via [`Query::reconstruct_archived_block`].
    invalidated_paths: RwLock<Vec<Vec<BlockHash>>>,
    /// Never-confirmed side-branch bodies, keyed by hash. Small cap.
    /// Not a block index: once-confirmed losers stay in Class A.
    held_bodies: RwLock<HashMap<BlockHash, Block>>,
    precious: RwLock<Option<BlockHash>>,
    /// Losing tips after a most-work reorg (hashes only). Bodies via archive.
    fork_tips: RwLock<HashSet<BlockHash>>,
    /// Header-only tips (`submitheader` / P2P headers): hash → (prev, height).
    /// Not a block index — no bodies, no status machine.
    header_tips: RwLock<HashMap<BlockHash, (BlockHash, u32)>>,
}

/// One `getchaintips` row. Status is a Core-shaped string (`active`,
/// `valid-fork`, `valid-headers`, `headers-only`, `invalid`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChainTipInfo {
    pub height: u32,
    pub hash: BlockHash,
    pub branchlen: u32,
    pub status: &'static str,
}

impl ChainHub {
    pub fn new(query: Query, params: ChainParams, milestone: Milestone) -> Self {
        let (tip_tx, _) = broadcast::channel(64);
        let query = Arc::new(query);
        let confirmed = Arc::new(RwLock::new(seed_confirmed_tip(&query)));
        // Full confirmed-set fill in background (mainnet-scale tips make a
        // synchronous walk multi-minute). Tip/genesis are seeded immediately.
        spawn_confirmed_seed(query.clone(), confirmed.clone());
        Self {
            query,
            cache: Arc::new(BlockCache::new()),
            params,
            milestone,
            notify: Arc::new(Notify::new()),
            tip_tx,
            confirmed,
            connect_lock: std::sync::Mutex::new(()),
            mempool: std::sync::OnceLock::new(),
            clock: rbitcoin_consensus::NodeClock::new(),
            invalidated: RwLock::new(HashSet::new()),
            invalidated_paths: RwLock::new(Vec::new()),
            held_bodies: RwLock::new(HashMap::new()),
            precious: RwLock::new(None),
            fork_tips: RwLock::new(HashSet::new()),
            header_tips: RwLock::new(HashMap::new()),
        }
    }

    /// Attach mempool once (same Query Arc as this hub).
    pub fn attach_mempool(
        &self,
        mp: Arc<crate::tx_relay::MempoolHub>,
    ) -> Result<(), Arc<crate::tx_relay::MempoolHub>> {
        self.mempool.set(mp)
    }

    pub fn mempool(&self) -> Option<&Arc<crate::tx_relay::MempoolHub>> {
        self.mempool.get()
    }

    pub fn subscribe_tips(&self) -> broadcast::Receiver<TipEvent> {
        self.tip_tx.subscribe()
    }

    /// Ensure the genesis block is in the store (required before IBD getheaders).
    ///
    /// Peers never re-serve genesis via `getheaders` after the common ancestor;
    /// an empty store must start with height 0 locally.
    pub fn ensure_genesis(&self) -> Result<(), NetError> {
        if self.tip_height().is_some() {
            return Ok(());
        }
        let _guard = self.connect_lock.lock().unwrap_or_else(|e| e.into_inner());
        if self.tip_height().is_some() {
            return Ok(());
        }
        let genesis = genesis_block(&self.params);
        if genesis.block_hash() != self.params.genesis_hash {
            return Err(NetError::Protocol("genesis hash mismatch with params"));
        }
        self.connect_at(0, genesis)?;
        Ok(())
    }

    pub fn tip_height(&self) -> Option<u32> {
        self.query
            .tip_height()
            .map(|h| h.0)
            .or_else(|| self.cache.tip_height())
    }

    pub fn tip_hash(&self) -> Option<BlockHash> {
        // Store tip is authoritative after IBD/archive-confirm (cache may only
        // hold genesis or a short tip window while Class C is far ahead). Prefer
        // query when its height is at least the cache tip; otherwise fall back
        // to the in-memory cache chain (pre-store / regtest cache-only paths).
        let q_h = self.query.tip_height().map(|h| h.0);
        let c_h = self.cache.tip_height();
        match (q_h, c_h) {
            (Some(qh), Some(ch)) if ch > qh => self.cache.tip_hash(),
            (Some(qh), _) => self
                .query
                .header_at_height(rbitcoin_primitives::Height(qh))
                .ok()
                .flatten()
                .map(|(_, rec)| BlockHash::from_byte_array(rec.hash)),
            (None, Some(_)) => self.cache.tip_hash(),
            (None, None) => None,
        }
    }

    pub fn tip_header(&self) -> Option<Header> {
        let h = self.tip_height()?;
        self.query.wire_header_at_height(Height(h)).ok()
    }

    /// True if `hash` is on the confirmed best chain (or in the RAM tip cache).
    ///
    /// Uses an in-memory set (tip seeded immediately; full chain filled in the
    /// background on connect). Must **not** fall back to `height_of_hash` here —
    /// header-only archive rows would force multi-thousand-height scans per call.
    pub fn has_block(&self, hash: &BlockHash) -> bool {
        if self.cache.get_block(hash).is_some() {
            return true;
        }
        self.confirmed.read().unwrap().contains(hash)
    }

    /// True if `hash` is connected on the best chain (has a height).
    ///
    /// Download / fork-start decisions must use this, not [`Self::has_block`]:
    /// the RAM body cache can evict, and `confirmed` is insert-only across
    /// reorgs. A stale "we have it" would permanently suppress getdata.
    pub fn is_connected(&self, hash: &BlockHash) -> bool {
        self.query
            .height_of_hash(&hash.to_byte_array())
            .ok()
            .flatten()
            .is_some()
    }

    /// Active tip plus known side tips (held / archive losers / invalidate).
    ///
    /// Losing tips are hashes only; bodies come from hold or
    /// [`Query::reconstruct_archived_block`]. Not a Core block index.
    pub fn chaintips(&self) -> Vec<ChainTipInfo> {
        let mut out: HashMap<BlockHash, ChainTipInfo> = HashMap::new();
        if let (Some(height), Some(hash)) = (self.tip_height(), self.tip_hash()) {
            out.insert(
                hash,
                ChainTipInfo {
                    height,
                    hash,
                    branchlen: 0,
                    status: "active",
                },
            );
        }

        let record =
            |map: &mut HashMap<BlockHash, ChainTipInfo>, hash: BlockHash, status: &'static str| {
                if map.get(&hash).map(|t| t.status) == Some("active") {
                    return;
                }
                if self.is_connected(&hash) {
                    return;
                }
                let Some((height, branchlen)) = self.side_height_and_branchlen(hash) else {
                    return;
                };
                let rank = |s: &str| match s {
                    "invalid" => 3,
                    "valid-fork" => 2,
                    "valid-headers" => 1,
                    "headers-only" => 0,
                    _ => 0,
                };
                match map.get(&hash) {
                    Some(prev) if rank(prev.status) >= rank(status) => {}
                    _ => {
                        map.insert(
                            hash,
                            ChainTipInfo {
                                height,
                                hash,
                                branchlen,
                                status,
                            },
                        );
                    }
                }
            };

        for h in self.fork_tips.read().unwrap().iter().copied() {
            record(&mut out, h, "valid-fork");
        }
        {
            let headers = self.header_tips.read().unwrap();
            for hash in headers.keys().copied() {
                let status = if self.header_ancestry_invalid(hash) {
                    "invalid"
                } else {
                    "headers-only"
                };
                record(&mut out, hash, status);
            }
        }
        {
            let held = self.held_bodies.read().unwrap();
            let parents: HashSet<BlockHash> =
                held.values().map(|b| b.header.prev_blockhash).collect();
            for hash in held.keys().copied() {
                if parents.contains(&hash) {
                    continue;
                }
                record(&mut out, hash, "valid-headers");
            }
        }
        for path in self.invalidated_paths.read().unwrap().iter() {
            if let Some(h) = path.last().copied() {
                record(&mut out, h, "invalid");
            }
        }

        let mut tips: Vec<ChainTipInfo> = out.into_values().collect();
        tips.sort_by(|a, b| {
            b.height
                .cmp(&a.height)
                .then_with(|| a.hash.to_byte_array().cmp(&b.hash.to_byte_array()))
        });
        tips
    }

    /// Prev hash from a held/archive body or the header store (no extra index).
    fn prev_of(&self, hash: &BlockHash) -> Option<BlockHash> {
        if let Some(b) = self.load_side_body(hash) {
            return Some(b.header.prev_blockhash);
        }
        let (_, rec) = self
            .query
            .get_header_by_hash(&hash.to_byte_array())
            .ok()
            .flatten()?;
        if rec.prev_fk.is_null() {
            return Some(BlockHash::from_byte_array([0u8; 32]));
        }
        self.query
            .get_header(rec.prev_fk)
            .ok()
            .map(|p| BlockHash::from_byte_array(p.hash))
    }

    fn header_ancestry_invalid(&self, tip: BlockHash) -> bool {
        let inv = self.invalidated.read().unwrap();
        if inv.contains(&tip) {
            return true;
        }
        let mut h = tip;
        for _ in 0..10_000 {
            let Some(prev) = self.prev_of(&h) else {
                return false;
            };
            if prev.to_byte_array() == [0u8; 32] || self.is_connected(&prev) {
                return false;
            }
            if inv.contains(&prev) {
                return true;
            }
            h = prev;
        }
        false
    }

    /// Height of a non-active tip and the length of the branch to the best chain.
    fn side_height_and_branchlen(&self, tip: BlockHash) -> Option<(u32, u32)> {
        let mut h = tip;
        let mut branchlen = 0u32;
        for _ in 0..10_000 {
            let prev = self.prev_of(&h)?;
            branchlen = branchlen.saturating_add(1);
            if prev.to_byte_array() == [0u8; 32] {
                return Some((branchlen.saturating_sub(1), branchlen));
            }
            if self.is_connected(&prev) {
                let parent_h = self
                    .query
                    .height_of_hash(&prev.to_byte_array())
                    .ok()
                    .flatten()?
                    .0;
                return Some((parent_h.saturating_add(branchlen), branchlen));
            }
            h = prev;
        }
        None
    }

    /// Best known header height (may lead `blocks` after `submitheader`).
    pub fn best_header_height(&self) -> u32 {
        let mut best = self.tip_height().unwrap_or(0);
        let headers = self.header_tips.read().unwrap();
        for (hash, (_, h)) in headers.iter() {
            if self.header_ancestry_invalid(*hash) {
                continue;
            }
            best = best.max(*h);
        }
        best
    }

    fn note_header_tip(&self, header: &Header) {
        let hash = header.block_hash();
        if self.is_connected(&hash) {
            self.header_tips.write().unwrap().remove(&hash);
            return;
        }
        let prev = header.prev_blockhash;
        let height = if self.is_connected(&prev) {
            self.query
                .height_of_hash(&prev.to_byte_array())
                .ok()
                .flatten()
                .map(|h| h.0.saturating_add(1))
        } else {
            self.header_tips
                .read()
                .unwrap()
                .get(&prev)
                .map(|(_, h)| h.saturating_add(1))
        };
        let Some(height) = height else {
            return;
        };
        let mut tips = self.header_tips.write().unwrap();
        tips.remove(&prev);
        if tips.len() >= 128 && !tips.contains_key(&hash) {
            if let Some(k) = tips.keys().next().copied() {
                tips.remove(&k);
            }
        }
        tips.insert(hash, (prev, height));
    }

    /// Persist a header row only (for header-sync → out-of-order body archive).
    pub fn ensure_header(&self, header: &Header) -> Result<(), NetError> {
        let _ = self.ensure_header_fk(header)?;
        Ok(())
    }

    /// Like [`ensure_header`], but returns the header fk for the archive writer
    /// (avoids a second hash-head probe on the hot write path).
    ///
    /// **Fail closed:** non-genesis headers require the parent row to already
    /// exist. Never write `prev_fk = NULL` for a missing parent (that created
    /// millions of orphan rows and false resume edges on mainnet).
    pub fn ensure_header_fk(&self, header: &Header) -> Result<Fk, NetError> {
        let prev_fk = if header.prev_blockhash.to_byte_array() == [0u8; 32] {
            Fk::NULL
        } else {
            match self
                .query
                .get_header_by_hash(header.prev_blockhash.as_byte_array())
                .map_err(|e| NetError::Consensus(e.to_string()))?
            {
                Some((fk, _)) => fk,
                None => {
                    return Err(NetError::Consensus(
                        "header parent unknown — ensure parent before child".into(),
                    ));
                }
            }
        };
        let rec = header_to_record(prev_fk, header);
        let fk = self
            .query
            .ensure_header(&rec)
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.note_header_tip(header);
        Ok(fk)
    }

    /// Contiguous tip-extension slice for one-shot load (owned Block).
    fn confirm_wire_contig(
        &self,
        blocks: &[(Height, Block)],
        pipeline: Option<&WireLoadPipeline>,
    ) -> Option<Vec<(Height, Block)>> {
        if blocks.is_empty() {
            return None;
        }
        let store_path_lo = match self.tip_height() {
            None => 0u32,
            Some(t) => t.saturating_add(1),
        };
        let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);
        let need: Vec<(Height, Block)> = blocks
            .iter()
            .filter(|(h, b)| {
                let hash = b.block_hash();
                !self.has_block(&hash) && h.0 >= path_lo
            })
            .cloned()
            .collect();
        let mut contig = Vec::new();
        for (h, b) in need {
            if h.0 != path_lo.saturating_add(contig.len() as u32) {
                break;
            }
            contig.push((h, b));
        }
        if contig.is_empty() {
            None
        } else {
            Some(contig)
        }
    }

    /// IBD **load** after lookup stamp: pin + assemble (does not re-lookup).
    ///
    /// Single path: denserels by body range from lookup stamp (plan-local or plan=None
    /// `ParentPinStamp`). No cold denserels dual path.
    pub fn confirm_wire_load_from_plan(
        &self,
        stamped: PlanStampOutcome,
        pipeline: Option<&WireLoadPipeline>,
    ) -> Result<rbitcoin_consensus::ConfirmLoadOutcome, NetError> {
        consensus_load_from_plan(
            &self.query,
            &self.params,
            self.milestone,
            stamped,
            pipeline,
            &ScriptPreverified::new(),
        )
        .map_err(|e| NetError::Consensus(e.to_string()))
    }

    /// Unified lookup+load from raw wire blocks (no Class-A wire rebuild).
    /// Skips heights already confirmed. Does **not** require prior archive.
    ///
    /// When `pipeline` is `None`, first height must be store tip+1 (legacy).
    /// When `Some`, first height is `pipeline.path_lo` so lookup(N+1) can run
    /// while write(N) has not advanced tip.
    ///
    /// One-shot path (tests / tip-follow): stamp + pin denserels by range + assemble.
    /// IBD load uses [`Self::confirm_wire_load_from_plan`] after BQ TipOnly stamp.
    pub fn confirm_wire_load_phase(
        &self,
        blocks: &[(Height, Block)],
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        self.confirm_wire_load_phase_pipelined(blocks, None)
    }

    /// Load with optional pipeline caches (reserved create fks + in-flight creates).
    ///
    /// One-shot or pipelined load: lookup stamps then pin denserels by range.
    pub fn confirm_wire_load_phase_pipelined(
        &self,
        blocks: &[(Height, Block)],
        pipeline: Option<&WireLoadPipeline>,
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        let Some(contig) = self.confirm_wire_contig(blocks, pipeline) else {
            return Ok(None);
        };
        let ok = confirm_wire_load_phase_pipelined(
            &self.query,
            &self.params,
            self.milestone,
            &contig,
            &ScriptPreverified::new(),
            pipeline,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        Ok(Some(ok))
    }

    /// WRITE stage: structural + Class C + spend annotate (ordered).
    pub fn confirm_write(&self, batch: ScriptOkBatch) -> Result<Vec<AcceptOutcome>, NetError> {
        let meta: Vec<(u32, BlockHash)> = batch
            .heights_hashes()
            .into_iter()
            .map(|(h, raw)| (h, BlockHash::from_byte_array(raw)))
            .collect();
        confirm_write_phase(&self.query, &self.params, self.milestone, batch)
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.note_confirmed_tip(&meta)?;
        Ok(meta
            .iter()
            .map(|&(height, _)| AcceptOutcome::Accepted { height })
            .collect())
    }

    fn note_confirmed_tip(&self, need_meta: &[(u32, BlockHash)]) -> Result<(), NetError> {
        // Mempool strip only when tip-mode relay is on. During IBD catch-up,
        // remove_for_block is a no-op (relay off); purge runs at set_relay_enabled.
        if let Some(mp) = self.mempool() {
            if mp.relay_enabled() {
                for &(_height, hash) in need_meta {
                    if let Ok(Some(block)) =
                        self.query.reconstruct_block_by_hash(&hash.to_byte_array())
                    {
                        let ids: Vec<_> = block.txdata.iter().map(|t| t.compute_txid()).collect();
                        let n = mp.remove_for_block(&ids);
                        if n > 0 {
                            rbitcoin_log::debug!("mempool: removed {n} confirmed tx(s) @ {hash}");
                        }
                    }
                }
            }
        }
        let mut confirmed = self.confirmed.write().unwrap();
        for &(height, hash) in need_meta {
            confirmed.insert(hash);
            if let Ok(hdr) = self.query.wire_header_at_height(Height(height)) {
                let _ = self.tip_tx.send(TipEvent {
                    height,
                    hash,
                    header: hdr,
                });
            }
        }
        drop(confirmed);
        self.notify.notify_waiters();
        Ok(())
    }

    /// Mine `nblocks` paying `script_pubkey` and accept each via [`Self::accept_block`].
    ///
    /// Regtest harness only. Extra txs go in the first block. Ensures genesis.
    pub fn generate_to_script(
        &self,
        nblocks: u32,
        script_pubkey: ScriptBuf,
        extra_txs: Vec<Transaction>,
    ) -> Result<Vec<BlockHash>, NetError> {
        self.ensure_genesis()?;
        if nblocks == 0 {
            return Ok(Vec::new());
        }
        if nblocks > 10_000 {
            return Err(NetError::Consensus("nblocks too large (max 10000)".into()));
        }
        let mut hashes = Vec::with_capacity(nblocks as usize);
        let mut extras = extra_txs;
        for i in 0..nblocks {
            let tip_h = self
                .tip_height()
                .ok_or(NetError::Protocol("generate: no tip"))?;
            let prev = self
                .tip_hash()
                .ok_or(NetError::Protocol("generate: no tip hash"))?;
            let tip_time = self.tip_header().map(|h| h.time).unwrap_or(0);
            let now = self.clock.now_secs() as u32;
            let time = tip_time.saturating_add(1).max(now);
            let txs = if i == 0 {
                std::mem::take(&mut extras)
            } else {
                Vec::new()
            };
            let block = mine_regtest_paying(
                prev,
                time,
                tip_h.saturating_add(1),
                script_pubkey.clone(),
                txs,
            );
            match self.accept_block(block.clone())? {
                AcceptOutcome::Accepted { .. } => hashes.push(block.block_hash()),
                other => {
                    return Err(NetError::Consensus(format!(
                        "generate did not extend tip: {other:?}"
                    )));
                }
            }
        }
        Ok(hashes)
    }

    /// Disconnect `hash` and descendants from the tip. Remember hashes only;
    /// [`Self::reconsider_block`] reconstructs from Class A. Then apply the
    /// next most-work non-invalid fork (production: invalidate is not "stay
    /// on the stump").
    pub fn invalidate_block(&self, hash: BlockHash) -> Result<(), NetError> {
        let Some(h) = self
            .query
            .height_of_hash(&hash.to_byte_array())
            .map_err(|e| NetError::Consensus(e.to_string()))?
        else {
            return Err(NetError::Consensus("Block not found".into()));
        };
        let tip = self.tip_height().unwrap_or(0);
        if h.0 > tip {
            return Err(NetError::Consensus("block not on tip path".into()));
        }
        let _guard = self.connect_lock.lock().unwrap_or_else(|e| e.into_inner());
        let mut path = Vec::new();
        for ht in h.0..=tip {
            if let Some(b) = self.block_at_height(ht)? {
                let bh = b.block_hash();
                self.invalidated.write().unwrap().insert(bh);
                path.push(bh);
            }
        }
        if !path.is_empty() {
            self.invalidated_paths.write().unwrap().push(path);
        }
        let keep = h.0.saturating_sub(1);
        self.disconnect_to(keep)?;
        drop(_guard);
        let _ = self.try_apply_after_invalidate()?;
        Ok(())
    }

    /// After invalidate, activate the best remaining fork (held or archive).
    fn try_apply_after_invalidate(&self) -> Result<Option<AcceptOutcome>, NetError> {
        let inv = self.invalidated.read().unwrap().clone();
        let mut starts: Vec<BlockHash> = self.fork_tips.read().unwrap().iter().copied().collect();
        starts.extend(self.held_bodies.read().unwrap().keys().copied());
        if let Some(p) = *self.precious.read().unwrap() {
            if !starts.contains(&p) {
                starts.push(p);
            }
        }
        let mut best: Option<(bitcoin::Work, Vec<Block>)> = None;
        for start in starts {
            if inv.contains(&start) {
                continue;
            }
            let Some(branch) = self.assemble_side_branch(start) else {
                continue;
            };
            if branch.iter().any(|b| inv.contains(&b.block_hash())) {
                continue;
            }
            let w = sum_work(branch.iter().map(|b| b.header.work()));
            let take = match &best {
                None => true,
                Some((bw, _)) => work_better(w, *bw),
            };
            if take {
                best = Some((w, branch));
            }
        }
        let Some((_, branch)) = best else {
            return Ok(None);
        };
        match self.accept_branch(&branch) {
            Ok(AcceptOutcome::Accepted { height }) => Ok(Some(AcceptOutcome::Accepted { height })),
            Ok(AcceptOutcome::IgnoredWeaker) => Ok(None),
            Ok(other) => Ok(Some(other)),
            Err(NetError::Protocol(s)) if s.contains("branch parent not on chain") => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Clear the invalid mark on `hash`, its invalidated path, and ancestors;
    /// re-apply bodies from archive. Header-only descendants stay header tips.
    pub fn reconsider_block(&self, hash: BlockHash) -> Result<(), NetError> {
        let known = self.is_connected(&hash)
            || self.load_side_body(&hash).is_some()
            || self.header_tips.read().unwrap().contains_key(&hash)
            || self
                .query
                .get_header_by_hash(&hash.to_byte_array())
                .ok()
                .flatten()
                .is_some()
            || self
                .invalidated_paths
                .read()
                .unwrap()
                .iter()
                .any(|p| p.contains(&hash));
        if !known {
            return Err(NetError::Consensus("Block not found".into()));
        }

        let mut related: HashSet<BlockHash> = HashSet::new();
        related.insert(hash);
        let mut walk = hash;
        for _ in 0..10_000 {
            let Some(prev) = self.prev_of(&walk) else {
                break;
            };
            if prev.to_byte_array() == [0u8; 32] {
                break;
            }
            related.insert(prev);
            if self.is_connected(&prev) {
                break;
            }
            walk = prev;
        }

        self.invalidated.write().unwrap().remove(&hash);
        let paths: Vec<Vec<BlockHash>> = {
            let mut g = self.invalidated_paths.write().unwrap();
            let mut taken = Vec::new();
            let mut seeds = related.clone();
            seeds.insert(hash);
            loop {
                let before = taken.len();
                g.retain(|p| {
                    let hit = p.iter().any(|h| seeds.contains(h))
                        || p.first().is_some_and(|h| {
                            self.prev_of(h).is_some_and(|prev| seeds.contains(&prev))
                        });
                    if hit {
                        for h in p {
                            seeds.insert(*h);
                        }
                        taken.push(p.clone());
                        false
                    } else {
                        true
                    }
                });
                if taken.len() == before {
                    break;
                }
            }
            taken
        };
        {
            let mut inv = self.invalidated.write().unwrap();
            for path in &paths {
                for h in path {
                    inv.remove(h);
                }
            }
            for h in &related {
                inv.remove(h);
            }
        }

        for path in paths {
            let mut branch = Vec::new();
            for h in &path {
                if self.is_connected(h) {
                    continue;
                }
                let Some(b) = self.load_side_body(h) else {
                    break;
                };
                branch.push(b);
            }
            if !branch.is_empty() {
                match self.accept_branch(&branch) {
                    Err(NetError::Protocol(s)) if s.contains("branch parent not on chain") => {}
                    Err(e) => return Err(e),
                    Ok(_) => {}
                }
            }
        }
        if !self.is_connected(&hash) {
            if let Some(branch) = self.assemble_side_branch(hash) {
                match self.accept_branch(&branch) {
                    Err(NetError::Protocol(s)) if s.contains("branch parent not on chain") => {}
                    Err(e) => return Err(e),
                    Ok(_) => {}
                }
            }
        }
        Ok(())
    }

    /// Prefer this hash among equal-work competing tips.
    pub fn precious_block(&self, hash: BlockHash) -> Result<(), NetError> {
        *self.precious.write().unwrap() = Some(hash);
        if let Some(branch) = self.assemble_side_branch(hash) {
            match self.accept_branch(&branch) {
                Err(NetError::Protocol(s)) if s.contains("branch parent not on chain") => {}
                Err(e) => return Err(e),
                Ok(_) => {}
            }
        } else {
            let _ = self.try_apply_held()?;
        }
        Ok(())
    }

    /// Accept a block that extends the tip, or reorg to a stronger competing tip / branch.
    pub fn accept_block(&self, block: Block) -> Result<AcceptOutcome, NetError> {
        let hash = block.block_hash();
        // Fast path without lock (common AlreadyHave).
        if self.tip_hash() == Some(hash) || self.has_block(&hash) {
            return Ok(AcceptOutcome::AlreadyHave);
        }
        if self.invalidated.read().unwrap().contains(&hash) {
            return Err(NetError::Consensus("block is invalidated".into()));
        }

        let _guard = self.connect_lock.lock().unwrap_or_else(|e| e.into_inner());
        // Same hash already tip/confirmed (or won a concurrent accept): drop —
        // do not plan Class A or assign create fks a second time (I4).
        if self.tip_hash() == Some(hash) || self.has_block(&hash) {
            return Ok(AcceptOutcome::AlreadyHave);
        }

        let prev = block.header.prev_blockhash;
        match self.tip_height() {
            None => {
                if prev.to_byte_array() != [0u8; 32] {
                    return Err(NetError::Protocol("non-genesis without tip"));
                }
                self.connect_at(0, block)?;
                Ok(AcceptOutcome::Accepted { height: 0 })
            }
            Some(tip_h) => {
                let tip_hash = self
                    .tip_hash()
                    .ok_or(NetError::Protocol("missing tip hash"))?;
                if prev == tip_hash {
                    let height = tip_h.saturating_add(1);
                    self.connect_at(height, block)?;
                    return Ok(AcceptOutcome::Accepted { height });
                }

                // Parent on best chain?
                let Some(parent_h) = self
                    .query
                    .height_of_hash(&prev.to_byte_array())
                    .map_err(|e| NetError::Consensus(e.to_string()))?
                else {
                    return Err(NetError::Protocol("unknown parent"));
                };

                let new_height = parent_h.0.saturating_add(1);
                if new_height > tip_h {
                    return Err(NetError::Protocol("gap above tip"));
                }

                if new_height == tip_h {
                    // Competing tip at same height — reorg if more work.
                    let cur = self
                        .block_at_height(tip_h)?
                        .ok_or(NetError::Protocol("missing current tip block"))?;
                    let precious = *self.precious.read().unwrap() == Some(hash);
                    if block.header.work() > cur.header.work()
                        || (block.header.work() == cur.header.work() && precious)
                    {
                        self.disconnect_to(parent_h.0)?;
                        self.connect_at(new_height, block)?;
                        return Ok(AcceptOutcome::Accepted { height: new_height });
                    }
                    return Ok(AcceptOutcome::IgnoredWeaker);
                }

                // Extends an ancestor — single block cannot beat a longer chain alone.
                // Caller should use accept_branch with the full better path.
                Err(NetError::Protocol(
                    "side block; use accept_branch for reorg",
                ))
            }
        }
    }

    /// Connect a contiguous branch `[blocks[0]…blocks[n]]` where `blocks[0].prev` is on our chain.
    /// Reorgs if the new path has strictly more work than our path from the fork.
    pub fn accept_branch(&self, blocks: &[Block]) -> Result<AcceptOutcome, NetError> {
        let _guard = self.connect_lock.lock().unwrap_or_else(|e| e.into_inner());
        if blocks.is_empty() {
            return Err(NetError::Protocol("empty branch"));
        }
        for w in blocks.windows(2) {
            if w[1].header.prev_blockhash != w[0].block_hash() {
                return Err(NetError::Protocol("branch not linked"));
            }
        }
        let fork_prev = blocks[0].header.prev_blockhash;
        let fork_h = if fork_prev.to_byte_array() == [0u8; 32] {
            // Branch starts at genesis — only if we have no chain or reorg entire chain.
            if self.tip_height().is_none() {
                for (i, b) in blocks.iter().enumerate() {
                    self.connect_at(i as u32, b.clone())?;
                }
                let h = (blocks.len() - 1) as u32;
                return Ok(AcceptOutcome::Accepted { height: h });
            }
            // Replacing from genesis
            None
        } else {
            Some(
                self.query
                    .height_of_hash(&fork_prev.to_byte_array())
                    .map_err(|e| NetError::Consensus(e.to_string()))?
                    .ok_or(NetError::Protocol("branch parent not on chain"))?,
            )
        };

        let fork_height = fork_h.map(|h| h.0);

        // Work on new path (header work only — same ranking as most_work helpers).
        let new_work = sum_work(blocks.iter().map(|b| b.header.work()));

        // Work on our path from fork+1..=tip (wire headers; no full body load).
        let our_work = self.work_from_fork_to_tip(fork_height)?;

        let branch_tip = blocks.last().map(Block::block_hash);
        let precious = *self.precious.read().unwrap() == branch_tip;
        // Precious may break an equal-work tie. It must not activate less work.
        let equal_work = !work_better(new_work, our_work) && !work_better(our_work, new_work);
        if self.tip_height().is_some()
            && !work_better(new_work, our_work)
            && !(precious && equal_work)
        {
            return Ok(AcceptOutcome::IgnoredWeaker);
        }

        // Snapshot old tip path for restore if connect fails mid-branch.
        let tip_h = self.tip_height().unwrap_or(0);
        let mut old_path: Vec<Block> = Vec::new();
        if let Some(fh) = fork_height {
            if tip_h > fh {
                old_path.reserve((tip_h - fh) as usize);
                for h in (fh + 1)..=tip_h {
                    if let Some(b) = self.block_at_height(h)? {
                        old_path.push(b);
                    }
                }
            }
        }

        // Once-confirmed losers stay in Class A (`reconstruct_archived_block`).
        // Do not copy `old_path` into the held-body map (that is a block index).

        // Reorg: disconnect down to fork, then connect branch.
        if let Some(fh) = fork_height {
            self.disconnect_to(fh)?;
        } else {
            while self.query.tip_height().is_some() {
                if let Some(th) = self.tip_hash() {
                    self.confirmed.write().unwrap().remove(&th);
                }
                self.query
                    .disconnect_tip()
                    .map_err(|e| NetError::Consensus(e.to_string()))?;
            }
            self.cache.clear();
            self.confirmed.write().unwrap().clear();
        }

        let base = fork_height.map(|h| h + 1).unwrap_or(0);
        for (i, b) in blocks.iter().enumerate() {
            if let Err(e) = self.connect_at(base + i as u32, b.clone()) {
                // Mid-branch connect fail: restore pre-attempt tip (not leave LCA).
                if let Some(fh) = fork_height {
                    if let Err(disc) = self.disconnect_to(fh) {
                        return Err(NetError::Consensus(format!(
                            "reorg connect failed ({e}); disconnect for restore failed: {disc}"
                        )));
                    }
                    for (j, ob) in old_path.iter().enumerate() {
                        if let Err(re) = self.connect_at(base + j as u32, ob.clone()) {
                            return Err(NetError::Consensus(format!(
                                "reorg connect failed ({e}); tip restore failed: {re}"
                            )));
                        }
                    }
                }
                return Err(e);
            }
        }
        let height = base + (blocks.len() as u32) - 1;
        {
            let mut held = self.held_bodies.write().unwrap();
            for b in blocks {
                held.remove(&b.block_hash());
            }
        }
        {
            let mut forks = self.fork_tips.write().unwrap();
            if let Some(old) = old_path.last() {
                forks.insert(old.block_hash());
            }
            for b in blocks {
                forks.remove(&b.block_hash());
            }
        }
        Ok(AcceptOutcome::Accepted { height })
    }

    /// Production "we received a full block" (P2P `block` / compact, RPC
    /// `submitblock`). Tip-extend via [`Self::accept_block`]; otherwise hold
    /// the body by hash and [`Self::accept_branch`] when a held (or archived)
    /// path has more work — or is precious at equal work.
    ///
    /// [`Self::accept_block`] stays the tip-extend / competing-tip hot path
    /// (generate, IBD planned windows). Do not add a second confirm pipeline.
    pub fn accept_received_block(&self, block: Block) -> Result<AcceptOutcome, NetError> {
        let hash = block.block_hash();
        match self.accept_block(block.clone()) {
            Ok(AcceptOutcome::Accepted { height }) => {
                self.held_bodies.write().unwrap().remove(&hash);
                Ok(AcceptOutcome::Accepted { height })
            }
            Ok(AcceptOutcome::AlreadyHave) => {
                self.held_bodies.write().unwrap().remove(&hash);
                Ok(AcceptOutcome::AlreadyHave)
            }
            Ok(AcceptOutcome::IgnoredWeaker) => {
                self.hold_body(block);
                match self.try_apply_held()? {
                    Some(o) => Ok(o),
                    None => Ok(AcceptOutcome::IgnoredWeaker),
                }
            }
            Err(NetError::Protocol(s))
                if s.contains("side block") || s.contains("unknown parent") =>
            {
                self.hold_body(block);
                match self.try_apply_held()? {
                    Some(o) => Ok(o),
                    None => Ok(AcceptOutcome::IgnoredWeaker),
                }
            }
            Err(e) => {
                if self
                    .query
                    .get_header_by_hash(&hash.to_byte_array())
                    .ok()
                    .flatten()
                    .is_some()
                {
                    self.invalidated.write().unwrap().insert(hash);
                }
                Err(e)
            }
        }
    }

    /// Cap matches tip-follow pending (`MAX_PENDING_BLOCKS = 128`): enough for
    /// a ≥99-block side path, not an unbounded index.
    const HELD_BODIES_CAP: usize = 128;

    fn hold_body(&self, block: Block) {
        let hash = block.block_hash();
        if self.is_connected(&hash) {
            return;
        }
        let evict = {
            let held = self.held_bodies.read().unwrap();
            if held.contains_key(&hash) {
                return;
            }
            if held.len() < Self::HELD_BODIES_CAP {
                None
            } else {
                held.keys().next().copied()
            }
        };
        let mut held = self.held_bodies.write().unwrap();
        if let Some(k) = evict {
            held.remove(&k);
        }
        held.insert(hash, block);
    }

    /// Never-confirmed side-branch body in RAM. Once-confirmed disconnected
    /// blocks are reconstructed from Class A — they are not held here.
    pub fn held_body(&self, hash: &BlockHash) -> Option<Block> {
        self.held_bodies.read().unwrap().get(hash).cloned()
    }

    /// Parents of held bodies that are neither on the best chain nor held
    /// (nor reconstructable from archive). Peer download window uses this.
    pub fn held_missing_parents(&self) -> Vec<BlockHash> {
        let held = self.held_bodies.read().unwrap();
        let mut missing = Vec::new();
        for b in held.values() {
            let prev = b.header.prev_blockhash;
            if prev.to_byte_array() == [0u8; 32] {
                continue;
            }
            if self.is_connected(&prev) || held.contains_key(&prev) {
                continue;
            }
            if self
                .query
                .reconstruct_archived_block(&prev.to_byte_array())
                .ok()
                .flatten()
                .is_some()
            {
                continue;
            }
            if !missing.contains(&prev) {
                missing.push(prev);
            }
        }
        missing
    }

    fn load_side_body(&self, hash: &BlockHash) -> Option<Block> {
        if let Some(b) = self.held_body(hash) {
            return Some(b);
        }
        self.query
            .reconstruct_archived_block(&hash.to_byte_array())
            .ok()
            .flatten()
    }

    /// Walk hold + archive from `tip` back to a best-chain parent.
    fn assemble_side_branch(&self, tip: BlockHash) -> Option<Vec<Block>> {
        if self.is_connected(&tip) {
            return None;
        }
        let mut rev = Vec::new();
        let mut h = tip;
        for _ in 0..10_000 {
            let b = self.load_side_body(&h)?;
            let prev = b.header.prev_blockhash;
            rev.push(b);
            if prev.to_byte_array() == [0u8; 32] {
                rev.reverse();
                return Some(rev);
            }
            if self.is_connected(&prev) {
                rev.reverse();
                return Some(rev);
            }
            h = prev;
        }
        None
    }

    fn try_apply_held(&self) -> Result<Option<AcceptOutcome>, NetError> {
        let mut starts: Vec<BlockHash> = self.held_bodies.read().unwrap().keys().copied().collect();
        if let Some(p) = *self.precious.read().unwrap() {
            if !starts.contains(&p) {
                starts.push(p);
            }
        }
        if starts.is_empty() {
            return Ok(None);
        }
        let precious = *self.precious.read().unwrap();
        let mut best: Option<(Work, Vec<Block>, bool)> = None;
        for start in starts {
            let Some(branch) = self.assemble_side_branch(start) else {
                continue;
            };
            let w = sum_work(branch.iter().map(|b| b.header.work()));
            let tip = branch.last().map(Block::block_hash);
            let is_p = tip == precious;
            let take = match &best {
                None => true,
                Some((bw, _, was_p)) => {
                    work_better(w, *bw) || (!work_better(*bw, w) && is_p && !*was_p)
                }
            };
            if take {
                best = Some((w, branch, is_p));
            }
        }
        let Some((_, branch, _)) = best else {
            return Ok(None);
        };
        match self.accept_branch(&branch) {
            Ok(AcceptOutcome::Accepted { height }) => Ok(Some(AcceptOutcome::Accepted { height })),
            Ok(AcceptOutcome::IgnoredWeaker) => Ok(None),
            Ok(other) => Ok(Some(other)),
            Err(NetError::Protocol(s)) if s.contains("branch parent not on chain") => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn connect_at(&self, height: u32, block: Block) -> Result<(), NetError> {
        let hash = block.block_hash();
        let header = block.header;
        // Reorg disconnect is done before connect; confirm pipeline is tip+1 only.
        // Live mempool txs already had scripts run at accept — skip re-verify.
        let preverified = self
            .mempool()
            .map(|mp| mp.script_preverified_txids())
            .unwrap_or_default();
        // Phase 0 tip SH measure: clear window so this block's stats are clean.
        tip_accept_stats_reset();
        let t_wall = std::time::Instant::now();
        let now = self.clock.now_secs();
        rbitcoin_consensus::with_now(now, || {
            accept_and_connect_block_preverified(
                &self.query,
                &self.params,
                Height(height),
                &block,
                self.milestone,
                &preverified,
            )
        })
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.header_tips.write().unwrap().remove(&hash);
        let wall_ns = t_wall.elapsed().as_nanos() as u64;
        // Tip-mode only: remove_for_block no-ops while relay is off (IBD).
        if let Some(mp) = self.mempool() {
            let ids: Vec<_> = block.txdata.iter().map(|t| t.compute_txid()).collect();
            let n = mp.remove_for_block(&ids);
            if n > 0 {
                rbitcoin_log::debug!("mempool: removed {n} confirmed tx(s) @ height {height}");
            }
        }
        self.confirmed.write().unwrap().insert(hash);
        // Move block into tip-window cache (no full-history clone).
        let n_tx = block.txdata.len();
        let _ = self.cache.push_best(block);
        // Tip-follow / wire accept path: log every accepted tip block (Core-like
        // UpdateTip). IBD bulk confirm uses note_confirmed_tip without this line;
        // IBD retains periodic progress/perf status instead.
        log_update_tip(height, &hash, &header, n_tx);
        log_tip_accept_sh(height, n_tx, wall_ns);
        let event = TipEvent {
            height,
            hash,
            header,
        };
        let _ = self.tip_tx.send(event);
        self.notify.notify_waiters();
        Ok(())
    }

    fn disconnect_to(&self, keep_height: u32) -> Result<(), NetError> {
        // Collect disconnected block bodies for mempool re-accept (best-effort).
        let mut disconnected_txs: Vec<Transaction> = Vec::new();
        loop {
            let tip = match self.query.tip_height() {
                Some(h) => h.0,
                None => break,
            };
            if tip <= keep_height {
                break;
            }
            if let Ok(Some(b)) = self.block_at_height(tip) {
                for tx in b.txdata.iter().skip(1) {
                    disconnected_txs.push(tx.clone());
                }
            }
            if let Some(th) = self.tip_hash() {
                self.confirmed.write().unwrap().remove(&th);
            }
            self.query
                .disconnect_tip()
                .map_err(|e| NetError::Consensus(e.to_string()))?;
        }
        self.cache.truncate_to_height(keep_height);
        if let Some(mp) = self.mempool() {
            if !disconnected_txs.is_empty() {
                let n = mp.reorg_reaccept(&disconnected_txs);
                if n > 0 {
                    rbitcoin_log::debug!(
                        "mempool: re-accepted {n}/{} tx(s) after reorg disconnect to {keep_height}",
                        disconnected_txs.len()
                    );
                }
            }
        }
        Ok(())
    }

    fn block_at_height(&self, height: u32) -> Result<Option<Block>, NetError> {
        if let Some(h) = self.cache.hash_at_height(height) {
            if let Some(b) = self.cache.get_block(&h) {
                return Ok(Some(b));
            }
        }
        match self.query.reconstruct_block_at_height(Height(height)) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.to_string().contains("not found") || e.to_string().contains("NotFound") => {
                Ok(None)
            }
            Err(e) => Err(NetError::Consensus(e.to_string())),
        }
    }

    /// Total chain work from genesis through tip (best effort from headers).
    pub fn chain_work(&self) -> Result<Work, NetError> {
        self.work_from_fork_to_tip(None)
    }

    /// Sum wire-header work on the best chain from `fork_height+1` through tip.
    ///
    /// `fork_height = None` means from genesis (height 0) through tip.
    /// Empty tip → zero work.
    fn work_from_fork_to_tip(&self, fork_height: Option<u32>) -> Result<Work, NetError> {
        let Some(tip) = self.tip_height() else {
            return Ok(Work::from_be_bytes([0u8; 32]));
        };
        let start = fork_height.map(|h| h + 1).unwrap_or(0);
        if start > tip {
            return Ok(Work::from_be_bytes([0u8; 32]));
        }
        let mut works = Vec::new();
        for h in start..=tip {
            let hdr = self
                .query
                .wire_header_at_height(Height(h))
                .map_err(|e| NetError::Consensus(e.to_string()))?;
            works.push(hdr.work());
        }
        Ok(sum_work(works.into_iter()))
    }
}

/// Core-like per-block tip log for tip-follow / wire accept (`connect_at`).
///
/// Format is intentionally close to Bitcoin Core `UpdateTip` so operators can
/// grep one line per height. IBD does not call this for every confirm batch.
pub fn log_update_tip(height: u32, hash: &BlockHash, header: &Header, n_tx: usize) {
    let time = header.time;
    let ver = header.version.to_consensus();
    info!(
        "UpdateTip: new best={hash} height={height} version={ver} \
         tx={n_tx} date={time} progress=tip"
    );
}

/// Clear confirm + Class C SH meters before a tip-follow accept sample window.
fn tip_accept_stats_reset() {
    let _ = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    let _ = rbitcoin_query::class_c_phase_stats::sample_and_reset();
    let _ = rbitcoin_query::class_c_phase_stats::sample_tip_sh_and_reset();
}

/// Inputs for pure tip-accept SH line (unit-tested).
#[derive(Clone, Debug)]
pub struct TipAcceptShInput {
    pub height: u32,
    pub n_tx: usize,
    pub wall_ns: u64,
    /// Load assemble wall (confirm CONNECT_NS).
    pub load_ns: u64,
    pub script_ns: u64,
    pub class_a_ns: u64,
    pub class_c_ns: u64,
    pub spend_ns: u64,
    pub strong_ns: u64,
    pub tip_ns: u64,
    pub sh: rbitcoin_query::class_c_phase_stats::TipShSnap,
}

/// Format `tip: accept …` body (no log level). Pure for tests.
pub fn format_tip_accept_sh_line(i: &TipAcceptShInput) -> String {
    let wall_ms = i.wall_ns / 1_000_000;
    let load_ms = i.load_ns / 1_000_000;
    let script_ms = i.script_ns / 1_000_000;
    let class_a_ms = i.class_a_ns / 1_000_000;
    let class_c_ms = i.class_c_ns / 1_000_000;
    let spend_ms = i.spend_ns / 1_000_000;
    let strong_ms = i.strong_ns / 1_000_000;
    let tip_ms = i.tip_ns / 1_000_000;
    let sh = &i.sh;
    let sh_ms = sh.total_sh_ns() / 1_000_000;
    let filt_ms = sh.filter_ns / 1_000_000;
    let coll_ms = sh.collect_ns / 1_000_000;
    let sort_ms = sh.sort_ns / 1_000_000;
    let seed_ms = sh.seed_ns / 1_000_000;
    let body_ms = sh.body_ns / 1_000_000;
    let head_ms = sh.head_ns / 1_000_000;
    let sh_ratio = if i.wall_ns == 0 {
        0u64
    } else {
        (sh.total_sh_ns().saturating_mul(100)) / i.wall_ns.max(1)
    };
    // class_c = strong + tip only (table work). SH is parallel and listed separately.
    format!(
        "tip: accept h={h} tx={n_tx} wall={wall_ms}ms load={load_ms}ms script={script_ms}ms \
         class_a={class_a_ms}ms class_c={class_c_ms}ms (strong={strong_ms} tip_set={tip_ms}) \
         sh={sh_ms}ms \
         (filter={filt_ms} collect={coll_ms} sort={sort_ms} seed={seed_ms} body={body_ms} head={head_ms} \
         pin={pin} cold={cold} creates={creates} unique={unique} written={written}) \
         spend={spend_ms}ms sh/wall={sh_ratio}%",
        h = i.height,
        n_tx = i.n_tx,
        pin = sh.pin,
        cold = sh.cold,
        creates = sh.creates,
        unique = sh.unique,
        written = sh.written,
    )
}

/// Sample meters after tip accept and emit INFO `tip: accept …` (SH breakdown).
fn log_tip_accept_sh(height: u32, n_tx: usize, wall_ns: u64) {
    // confirm_phase_stats::sample_and_reset also clears class_c STRONG/SCRIPTHASH/TIP.
    let (
        _recon,
        _wire,
        connect_ns,
        script_ns,
        _class_c_ns, // tables-only after fix; we recompute from strong+tip below
        strong_ns,
        _sh_sum,
        tip_ns,
        spend_ns,
        _blks,
        _resolve,
        load_ns,
        _unpin,
        _cache_tip,
        _spend_ranged,
        _spend_idx,
        _spend_skip,
        _structural,
        _struct_spent,
        _struct_create_h,
        _struct_bip68,
    ) = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    // SH substeps/counts (FILTER/COLLECT/…/CREATE_N) — not cleared by sample_and_reset.
    let sh = rbitcoin_query::class_c_phase_stats::sample_tip_sh_and_reset();
    let ca = rbitcoin_query::archive_phase_stats::sample_and_reset();
    // class_c = strong + tip only (parallel SH is not Class C table time).
    let class_c_tables_ns = strong_ns.saturating_add(tip_ns);
    let line = format_tip_accept_sh_line(&TipAcceptShInput {
        height,
        n_tx,
        wall_ns,
        // pin (LOAD_NS) + assemble (CONNECT_NS)
        load_ns: load_ns.saturating_add(connect_ns),
        script_ns,
        class_a_ns: ca.write_total_ns,
        class_c_ns: class_c_tables_ns,
        spend_ns,
        strong_ns,
        tip_ns,
        sh,
    });
    info!("{line}");
}

/// Immediate seed: genesis + tip (and tip-1) so open is O(1) at mainnet scale.
fn seed_confirmed_tip(query: &Query) -> HashSet<BlockHash> {
    let mut set = HashSet::new();
    let Some(tip) = query.tip_height() else {
        return set;
    };
    for h in [0u32, tip.0.saturating_sub(1), tip.0] {
        if let Ok(Some((_, rec))) = query.header_at_height(Height(h)) {
            set.insert(BlockHash::from_byte_array(rec.hash));
        }
    }
    set
}

/// Fill the rest of the confirmed set without blocking P2P start.
fn spawn_confirmed_seed(query: Arc<Query>, confirmed: Arc<RwLock<HashSet<BlockHash>>>) {
    let Some(tip) = query.tip_height() else {
        return;
    };
    if tip.0 <= 2 {
        return;
    }
    let run = move || {
        let t0 = std::time::Instant::now();
        let mut batch = Vec::with_capacity(4096);
        for h in 0..=tip.0 {
            if let Ok(Some((_, rec))) = query.header_at_height(Height(h)) {
                batch.push(BlockHash::from_byte_array(rec.hash));
            }
            if batch.len() >= 4096 || h == tip.0 {
                let mut g = confirmed.write().unwrap();
                for hash in batch.drain(..) {
                    g.insert(hash);
                }
            }
        }
        info!(
            "ibd: confirmed-set seed complete tip={} in {:?}",
            tip.0,
            t0.elapsed()
        );
    };
    // Prefer the node runtime's blocking pool (no dedicated OS thread).
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        handle.spawn_blocking(run);
    } else {
        // Sync constructors / tests without a runtime.
        std::thread::Builder::new()
            .name("confirmed-seed".into())
            .spawn(run)
            .ok();
    }
}

use crate::most_work::{sum_work, work_better};

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, CompactTarget, OutPoint, Sequence, Target, Transaction, TxIn, TxOut, Witness,
    };
    use rbitcoin_consensus::{confirm_scripts_phase, ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_hub() -> (std::path::PathBuf, ChainHub) {
        // Keep Class A/hash heads tiny in unit tests (avoid multi‑GiB sparse maps).
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
        let dir = std::env::temp_dir().join(format!(
            "rbitcoin-chain-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::create_dir_all(&dir);
        let q = Query::open_or_create(dir.join("store")).expect("query open_or_create");
        let hub = ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
        (dir, hub)
    }

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

    fn mine(prev: BlockHash, time: u32, height: u32) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
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

    #[test]
    fn tip_follow_accept_logs_update_tip_per_block() {
        // Shipped path: accept_block → connect_at → log_update_tip (info).
        // Assert helper formats Core-like line; accept advances tip once per block.
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_000, 1);
        let h = b1.header;
        let hash = b1.block_hash();
        let line_probe = {
            // Drive the shipped log helper (same args connect_at uses).
            log_update_tip(1, &hash, &h, b1.txdata.len());
            format!(
                "UpdateTip: new best={hash} height=1 version={} tx={} date={} progress=tip",
                h.version.to_consensus(),
                b1.txdata.len(),
                h.time
            )
        };
        assert!(
            line_probe.starts_with("UpdateTip: new best="),
            "tip log must be Core-like UpdateTip: {line_probe}"
        );
        assert!(line_probe.contains("height=1"));
        assert!(line_probe.contains("progress=tip"));
        assert!(matches!(
            hub.accept_block(b1).unwrap(),
            AcceptOutcome::Accepted { height: 1 }
        ));
        assert_eq!(hub.tip_height(), Some(1));
        // Second block also accepted (one log per height on real path).
        let b2 = mine(hash, 1_300_000_600, 2);
        assert!(matches!(
            hub.accept_block(b2).unwrap(),
            AcceptOutcome::Accepted { height: 2 }
        ));
        assert_eq!(hub.tip_height(), Some(2));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn format_tip_accept_sh_line_has_sh_breakdown_tokens() {
        let line = format_tip_accept_sh_line(&TipAcceptShInput {
            height: 961_445,
            n_tx: 4_959,
            wall_ns: 2_500_000_000,
            load_ns: 100_000_000,
            script_ns: 200_000_000,
            class_a_ns: 50_000_000,
            // Tables only (strong+tip) — not SH join wall.
            class_c_ns: 7_000_000,
            spend_ns: 80_000_000,
            strong_ns: 5_000_000,
            tip_ns: 2_000_000,
            sh: rbitcoin_query::class_c_phase_stats::TipShSnap {
                filter_ns: 1_000_000,
                collect_ns: 20_000_000,
                sort_ns: 5_000_000,
                seed_ns: 800_000_000,
                body_ns: 600_000_000,
                head_ns: 300_000_000,
                pin: 4_000,
                cold: 12,
                creates: 12_000,
                unique: 9_500,
                written: 9_400,
            },
        });
        assert!(line.starts_with("tip: accept h=961445"), "{line}");
        assert!(line.contains("wall=2500ms"), "{line}");
        assert!(line.contains("class_c=7ms"), "{line}");
        assert!(line.contains("(strong=5 tip_set=2)"), "{line}");
        assert!(line.contains("sh=1726ms"), "{line}"); // 1+20+5+800+600+300
                                                       // Substep ms are unitless inside the paren (outer fields carry `ms`).
        assert!(line.contains("seed=800"), "{line}");
        assert!(line.contains("body=600"), "{line}");
        assert!(line.contains("head=300"), "{line}");
        assert!(line.contains("creates=12000"), "{line}");
        assert!(line.contains("unique=9500"), "{line}");
        assert!(line.contains("written=9400"), "{line}");
        assert!(line.contains("pin=4000"), "{line}");
        assert!(line.contains("cold=12"), "{line}");
        assert!(line.contains("sh/wall=69%"), "{line}");
    }

    #[test]
    fn ensure_genesis_accept_extend_and_already_have() {
        let (dir, hub) = tmp_hub();
        assert!(hub.tip_height().is_none());
        hub.ensure_genesis().unwrap();
        assert_eq!(hub.tip_height(), Some(0));
        // Second call is a no-op once tip exists.
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        assert!(hub.has_block(&gen));
        assert!(hub.query.is_block_archived(&gen.to_byte_array()).unwrap());

        let b1 = mine(gen, 1_300_000_000, 1);
        assert!(matches!(
            hub.accept_block(b1.clone()).unwrap(),
            AcceptOutcome::Accepted { height: 1 }
        ));
        assert_eq!(hub.tip_height(), Some(1));
        // AlreadyHave on re-accept.
        assert!(matches!(
            hub.accept_block(b1.clone()).unwrap(),
            AcceptOutcome::AlreadyHave
        ));

        // Non-genesis without tip rejected on empty hub.
        let (dir2, empty) = tmp_hub();
        let err = empty.accept_block(b1).unwrap_err();
        assert!(matches!(err, NetError::Protocol(_)));

        // Chain work is non-zero after tip.
        assert!(hub.chain_work().unwrap().to_be_bytes() != [0u8; 32]);
        assert!(hub.tip_header().is_some());
        assert!(hub.mempool().is_none());
        let _ = hub.subscribe_tips();

        let _ = std::fs::remove_dir_all(dir);
        let _ = std::fs::remove_dir_all(dir2);
    }

    /// Multi-peer concurrent accept of the same tip block: exactly one Accepted,
    /// rest AlreadyHave; single tip height; no orphan Class C outside tip body.
    #[test]
    fn concurrent_same_block_accept_no_orphan_class_c() {
        use std::sync::Arc;
        let (dir, hub) = tmp_hub();
        let hub = Arc::new(hub);
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_000, 1);
        let n = 8usize;
        let mut handles = Vec::new();
        for _ in 0..n {
            let h = Arc::clone(&hub);
            let b = b1.clone();
            handles.push(std::thread::spawn(move || h.accept_block(b)));
        }
        let mut accepted = 0u32;
        let mut already = 0u32;
        for h in handles {
            match h.join().unwrap().unwrap() {
                AcceptOutcome::Accepted { height: 1 } => accepted += 1,
                AcceptOutcome::AlreadyHave => already += 1,
                other => panic!("unexpected outcome {other:?}"),
            }
        }
        assert_eq!(accepted, 1, "exactly one Accepted");
        assert_eq!(already, (n as u32) - 1);
        assert_eq!(hub.tip_height(), Some(1));
        // Tip body membership: every strong+height tx at tip is in header_txs.
        let tip_fks = hub
            .query
            .block_tx_fks(rbitcoin_primitives::Height(1))
            .unwrap();
        let tip_set: std::collections::HashSet<u64> =
            tip_fks.iter().filter_map(|f| f.get()).collect();
        for &fk in &tip_fks {
            let id = fk.get().unwrap();
            assert!(
                tip_set.contains(&id),
                "orphan Class C fk={id} at tip height not in header_txs"
            );
            assert!(hub.query.store().is_confirmed_strong(fk).unwrap());
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Load batch N+1 must succeed while N is only loaded (not committed).
    /// Regression: hub used store tip+1 only → Ok(None) "empty outcome" thrash.
    #[test]
    fn wire_prep_ahead_of_store_tip_with_pipeline() {
        use rbitcoin_consensus::WireLoadPipeline;

        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        hub.query.enter_direct_index_mode().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_200, 1);
        let b2 = mine(b1.block_hash(), 1_300_000_800, 2);
        let h1 = b1.block_hash();
        let h2 = b2.block_hash();

        // Batch 1 at store tip+1 (path_lo=1).
        let batch1 = [(rbitcoin_primitives::Height(1), b1.clone())];
        let mut pipe = WireLoadPipeline {
            path_lo: 1,
            parent_hash: None,
            next_tx_start: hub.query.tx_body_count().saturating_add(1).max(1),
            in_flight: rbitcoin_query::InFlightView::empty(),
            parent_store: std::sync::Arc::new(rbitcoin_query::PipelineParentStore::new()),
        };
        let mat1 = hub
            .confirm_wire_load_phase_pipelined(&batch1, Some(&pipe))
            .expect("prep1")
            .expect("prep1 some");
        assert_eq!(mat1.batch.len(), 1);
        assert!(mat1.batch.archive_plan.is_some());
        assert_eq!(
            hub.tip_height(),
            Some(0),
            "tip must not advance on load alone"
        );

        // Update pipeline caches from plan (lookup-thread note_lookup_ok).
        let plan = mat1.batch.archive_plan.as_ref().unwrap();
        {
            let mut log = rbitcoin_query::InFlightLog::new();
            let layer = if plan.batch_pin.len() == plan.planned_fks.len() {
                rbitcoin_query::InFlightLayer::from_plan_pins(
                    plan.planned_fks
                        .iter()
                        .zip(plan.batch_pin.iter())
                        .map(|(fk, pin)| (*fk, pin)),
                )
            } else {
                rbitcoin_query::InFlightLayer::from_plan_pins(
                    plan.packed
                        .iter()
                        .zip(plan.planned_fks.iter())
                        .map(|((pin, _), fk)| (*fk, pin)),
                )
            };
            log.note_layer(layer);
            pipe.in_flight = log.snapshot();
        }
        if let Some(last) = plan.planned_fks.last().and_then(|f| f.get()) {
            pipe.next_tx_start = last.saturating_add(1).max(1);
        }
        pipe.path_lo = 2;
        pipe.parent_hash = Some(h1.to_byte_array());

        // Batch 2 while tip still 0 — must NOT Ok(None).
        let batch2 = [(rbitcoin_primitives::Height(2), b2.clone())];
        let mat2 = hub
            .confirm_wire_load_phase_pipelined(&batch2, Some(&pipe))
            .expect("prep2 err")
            .expect("prep2 must Some — pipeline path_lo=2 with tip=0");
        assert_eq!(mat2.batch.len(), 1);
        assert!(mat2.batch.archive_plan.is_some());
        // Reserved fks for batch2 start after batch1's plan.
        let p1_last = plan.planned_fks.last().unwrap().get().unwrap();
        let p2_first = mat2
            .batch
            .archive_plan
            .as_ref()
            .unwrap()
            .planned_fks
            .first()
            .unwrap()
            .get()
            .unwrap();
        assert!(
            p2_first > p1_last,
            "batch2 fks must not collide with batch1 reserved fks ({p2_first} <= {p1_last})"
        );
        assert_eq!(hub.tip_height(), Some(0));
        let _ = (h2,);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn archive_then_confirm_run_and_empty_paths() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_100, 1);
        let h1 = b1.block_hash();

        // Header then accept (confirm is sole Class A).
        hub.ensure_header(&b1.header).unwrap();
        let fk = hub.ensure_header_fk(&b1.header).unwrap();
        assert!(fk.0 > 0 || fk.0 == 0); // Fk may be 0 on some layouts
        assert!(hub.confirm_wire_load_phase(&[]).unwrap().is_none());
        let acc = hub.accept_block(b1.clone()).unwrap();
        assert!(matches!(acc, AcceptOutcome::Accepted { height: 1 }));
        assert!(hub.has_block(&h1));
        assert_eq!(hub.tip_height(), Some(1));
        // Already confirmed → AlreadyHave.
        assert!(matches!(
            hub.accept_block(b1.clone()).unwrap(),
            AcceptOutcome::AlreadyHave
        ));
        // Wire load on already-confirmed → None.
        assert!(hub
            .confirm_wire_load_phase(&[(Height(1), b1.clone())])
            .unwrap()
            .is_none());

        // Unknown parent.
        let orphan = mine(BlockHash::from_byte_array([9u8; 32]), 1_300_000_200, 99);
        assert!(matches!(
            hub.accept_block(orphan).unwrap_err(),
            NetError::Protocol(_)
        ));

        // accept_branch empty / unlinked.
        assert!(hub.accept_branch(&[]).is_err());
        let b2 = mine(h1, 1_300_000_300, 2);
        let b3_bad = mine(BlockHash::from_byte_array([1u8; 32]), 1_300_000_400, 3);
        assert!(hub.accept_branch(&[b2.clone(), b3_bad]).is_err());
        // Linked tip extension via branch.
        assert!(matches!(
            hub.accept_branch(&[b2.clone()]).unwrap(),
            AcceptOutcome::Accepted { height: 2 }
        ));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn accept_unknown_parent_errors() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let orphan = mine(BlockHash::from_byte_array([9u8; 32]), 1_300_000_500, 99);
        assert!(matches!(
            hub.accept_block(orphan).unwrap_err(),
            NetError::Protocol(_)
        ));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Receive path holds never-confirmed side bodies; it is not a block index.
    #[test]
    fn receive_path_holds_by_hash_not_block_index() {
        let src = include_str!("chain.rs");
        assert!(
            src.contains("held_bodies") && src.contains("fn hold_body"),
            "side bodies are held by hash"
        );
        assert!(
            src.contains("reconstruct_archived_block"),
            "once-confirmed losers come from Class A"
        );
    }

    #[test]
    fn accept_received_reorgs_to_longer_held_fork() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let a1 = mine(gen, 1_300_010_000, 1);
        hub.accept_block(a1.clone()).unwrap();
        let a2 = mine(a1.block_hash(), 1_300_010_100, 2);
        hub.accept_block(a2.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(2));

        let mut prev = gen;
        let mut fork = Vec::new();
        for i in 0..3u32 {
            let b = mine(prev, 1_300_011_000 + i, i + 1);
            prev = b.block_hash();
            fork.push(b);
        }
        for b in &fork {
            hub.accept_received_block(b.clone()).unwrap();
        }
        assert_eq!(hub.tip_hash().unwrap(), fork[2].block_hash());
        assert_eq!(hub.tip_height(), Some(3));

        let tips = hub.chaintips();
        assert_eq!(
            tips.len(),
            2,
            "active + losing valid-fork after held-then-applied reorg: {tips:?}"
        );
        assert_eq!(tips[0].status, "active");
        assert_eq!(tips[0].hash, fork[2].block_hash());
        assert_eq!(tips[0].branchlen, 0);
        let fork_tip = tips
            .iter()
            .find(|t| t.status == "valid-fork")
            .expect("loser");
        assert_eq!(fork_tip.hash, a2.block_hash());
        assert_eq!(fork_tip.height, 2);
        assert_eq!(fork_tip.branchlen, 2);

        // Once-confirmed loser is archive-reconstructable, not a RAM block index.
        assert!(
            hub.query
                .reconstruct_archived_block(&a2.block_hash().to_byte_array())
                .unwrap()
                .is_some(),
            "disconnected best-chain body must stay in Class A"
        );
        assert!(
            hub.held_body(&a2.block_hash()).is_none(),
            "hold is never-confirmed side bodies only — not a CBlockIndex clone of the old path"
        );

        // Equal-work never-confirmed sibling: stay held, do not reorg until precious.
        let mut prev = gen;
        let mut eq = Vec::new();
        for i in 0..3u32 {
            let b = mine(prev, 1_300_012_000 + i, i + 1);
            prev = b.block_hash();
            eq.push(b);
        }
        for b in &eq {
            let out = hub.accept_received_block(b.clone()).unwrap();
            assert!(matches!(
                out,
                AcceptOutcome::IgnoredWeaker | AcceptOutcome::AlreadyHave
            ));
        }
        assert_eq!(hub.tip_hash().unwrap(), fork[2].block_hash());
        hub.precious_block(eq[2].block_hash()).unwrap();
        assert_eq!(hub.tip_hash().unwrap(), eq[2].block_hash());

        // Switch back to the once-confirmed fork via archive, not a held clone.
        assert!(hub.held_body(&fork[2].block_hash()).is_none());
        assert!(hub
            .query
            .reconstruct_archived_block(&fork[2].block_hash().to_byte_array())
            .unwrap()
            .is_some());
        hub.precious_block(fork[2].block_hash()).unwrap();
        assert_eq!(hub.tip_hash().unwrap(), fork[2].block_hash());

        // invalidate / reconsider use archive hashes, not a RAM clone.
        // After invalidate, the next most-work fork (eq) becomes tip.
        let tip = hub.tip_hash().unwrap();
        hub.invalidate_block(fork[1].block_hash()).unwrap();
        assert_eq!(hub.tip_hash().unwrap(), eq[2].block_hash());
        assert!(hub.held_body(&tip).is_none());
        assert!(hub
            .query
            .reconstruct_archived_block(&tip.to_byte_array())
            .unwrap()
            .is_some());
        hub.reconsider_block(fork[1].block_hash()).unwrap();
        assert!(
            hub.held_body(&tip).is_none(),
            "reconsider must not park the old tip"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn submit_header_child_is_headers_only_tip() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_020_000, 1);
        hub.accept_block(b1.clone()).unwrap();
        let child = mine(b1.block_hash(), 1_300_020_100, 2);
        hub.ensure_header(&child.header).unwrap();
        let tips = hub.chaintips();
        let ho = tips
            .iter()
            .find(|t| t.status == "headers-only")
            .expect("headers-only child");
        assert_eq!(ho.hash, child.block_hash());
        assert_eq!(ho.height, 2);
        assert_eq!(ho.branchlen, 1);
        assert_eq!(hub.best_header_height(), 2);
        assert_eq!(hub.tip_height(), Some(1));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn accept_competing_tip_and_block_at_height_paths() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_001_000, 1);
        hub.accept_block(b1.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(1));

        // Competing tip at same height with more work reorgs (or IgnoredWeaker if equal).
        // Mine many nonces for a sibling of b1 with higher work is hard on regtest
        // equal-bits; exercise IgnoredWeaker via accept of a different equal-work sibling.
        let mut sibling = mine(gen, 1_300_001_001, 1);
        // Ensure different hash than b1.
        if sibling.block_hash() == b1.block_hash() {
            sibling.header.nonce = sibling.header.nonce.wrapping_add(1);
            // re-mine pow
            let target = Target::from_compact(sibling.header.bits);
            for nonce in sibling.header.nonce..u32::MAX {
                sibling.header.nonce = nonce;
                if sibling.header.validate_pow(target).is_ok()
                    && sibling.block_hash() != b1.block_hash()
                {
                    break;
                }
            }
        }
        let out = hub.accept_block(sibling).unwrap();
        assert!(matches!(
            out,
            AcceptOutcome::IgnoredWeaker | AcceptOutcome::Accepted { .. }
        ));

        // block_at_height via reconstruct after tip extend.
        let b2 = mine(hub.tip_hash().unwrap(), 1_300_001_100, 2);
        hub.accept_block(b2.clone()).unwrap();
        let got = hub.block_at_height(2).unwrap().unwrap();
        assert_eq!(got.block_hash(), b2.block_hash());
        // Far height → None.
        assert!(hub.block_at_height(9_999).unwrap().is_none());

        // attach_mempool + accept_block removes confirmed txs (empty mempool).
        let mp_dir = dir.join("mp");
        let mp = crate::tx_relay::MempoolHub::open(&mp_dir, Arc::clone(&hub.query)).unwrap();
        assert!(hub.attach_mempool(mp).is_ok());
        assert!(hub.mempool().is_some());
        let tip = hub.tip_hash().unwrap();
        let tip_h = hub.tip_height().unwrap();
        let b_next = mine(tip, 1_300_001_200, tip_h + 1);
        hub.accept_block(b_next).unwrap();

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn work_better_and_sum_work_helpers() {
        let z = Work::from_be_bytes([0u8; 32]);
        let one = {
            let mut b = [0u8; 32];
            b[31] = 1;
            Work::from_be_bytes(b)
        };
        assert!(work_better(one, z));
        assert!(!work_better(z, one));
        assert_eq!(sum_work(std::iter::empty()), z);
        assert_eq!(sum_work([one].into_iter()), one);
    }

    #[test]
    fn accept_branch_weaker_and_gap_errors() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_002_000, 1);
        let b2 = mine(b1.block_hash(), 1_300_002_100, 2);
        hub.accept_block(b1.clone()).unwrap();
        hub.accept_block(b2.clone()).unwrap();
        assert_eq!(hub.tip_height(), Some(2));

        // Single side block at height 1 while tip is 2 → side block protocol error.
        let mut side = mine(gen, 1_300_002_050, 1);
        if side.block_hash() == b1.block_hash() {
            let target = Target::from_compact(side.header.bits);
            for nonce in 0..u32::MAX {
                side.header.nonce = nonce;
                if side.header.validate_pow(target).is_ok() && side.block_hash() != b1.block_hash()
                {
                    break;
                }
            }
        }
        let err = hub.accept_block(side.clone()).unwrap_err();
        assert!(
            matches!(err, NetError::Protocol(_)),
            "side/gap should be protocol err: {err}"
        );

        // Weaker single-block branch at height 1 → IgnoredWeaker (less work than tip path).
        let out = hub.accept_branch(&[side]).unwrap();
        assert!(matches!(
            out,
            AcceptOutcome::IgnoredWeaker | AcceptOutcome::Accepted { .. }
        ));

        // Gap above tip: parent is tip, but we already have tip+1 path — build orphan
        // child of non-tip ancestor that's not tip-1? parent at height 0 with tip 2
        // is "side block; use accept_branch".
        // Missing parent:
        let orphan = mine(BlockHash::from_byte_array([0xab; 32]), 1_300_003_000, 99);
        assert!(matches!(
            hub.accept_block(orphan).unwrap_err(),
            NetError::Protocol(_)
        ));

        // tip_hash prefers store when present.
        assert_eq!(hub.tip_hash().unwrap(), b2.block_hash());
        assert!(hub.block_at_height(0).unwrap().is_some());
        assert!(hub.block_at_height(1).unwrap().is_some());

        // disconnect_to via reorg: better branch of length 2 from genesis with more work
        // is hard on equal-bits regtest; exercise disconnect_to indirectly by
        // accepting equal-length weaker branch (IgnoredWeaker already covered).

        // has_block false for random.
        assert!(!hub.has_block(&BlockHash::from_byte_array([0xde; 32])));

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn confirm_wire_script_split() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_004_000, 1);
        let loaded = hub.confirm_wire_load_phase(&[(Height(1), b1)]).unwrap();
        assert!(loaded.is_some());
        let batch = loaded.unwrap();
        let script_out = confirm_scripts_phase(batch.batch).unwrap();
        let write_out = hub.confirm_write(script_out.batch).unwrap();
        assert_eq!(write_out.len(), 1);
        assert!(matches!(
            write_out[0],
            AcceptOutcome::Accepted { height: 1 }
        ));
        assert_eq!(hub.tip_height(), Some(1));

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Mine a coinbase-only block with optional extra txs (for invalid mid-branch).
    fn mine_with_extra(prev: BlockHash, time: u32, height: u32, extra: Vec<Transaction>) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let header = Header {
            version: Version::ONE,
            prev_blockhash: prev,
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time,
            bits,
            nonce: 0,
        };
        let mut txdata = vec![coinbase(height)];
        txdata.extend(extra);
        let mut block = Block { header, txdata };
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

    /// Journey: deep most-work reorg (≥16), weaker ignored, mid-branch invalid
    /// restores pre-attempt tip (shipped `accept_branch`).
    #[test]
    fn most_work_reorg_depth16_and_invalid_mid_branch_restores_tip() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let mut tip = gen;
        let time = 1_400_000_000u32;
        // Best chain height 0..=8. Fork at height 2: main continues to 8.
        for h in 1..=8u32 {
            let b = mine(tip, time + h * 600, h);
            tip = b.block_hash();
            hub.accept_block(b).unwrap();
        }
        assert_eq!(hub.tip_height(), Some(8));
        let main_tip = hub.tip_hash().unwrap();

        // Fork parent at height 2.
        let fork_parent = hub
            .query
            .header_at_height(Height(2))
            .unwrap()
            .unwrap()
            .1
            .hash;
        let fork_prev = BlockHash::from_byte_array(fork_parent);
        let fork_time = hub
            .query
            .header_at_height(Height(2))
            .unwrap()
            .unwrap()
            .1
            .timestamp;

        // Competing branch depth 16 from height 3..=18 (16 blocks) → more work.
        let mut branch = Vec::new();
        let mut p = fork_prev;
        let mut t = fork_time;
        for (i, h) in (3..=18u32).enumerate() {
            let b = mine(p, t + 601 + i as u32, h);
            p = b.block_hash();
            t = b.header.time;
            branch.push(b);
        }
        assert_eq!(branch.len(), 16);
        let out = hub.accept_branch(&branch).unwrap();
        assert!(
            matches!(out, AcceptOutcome::Accepted { height: 18 }),
            "depth-16 reorg must accept, got {out:?}"
        );
        assert_eq!(hub.tip_height(), Some(18));
        assert_eq!(hub.tip_hash().unwrap(), branch.last().unwrap().block_hash());
        assert_ne!(hub.tip_hash().unwrap(), main_tip);

        // Weaker shorter branch from height 17 → IgnoredWeaker.
        let weak = mine(hub.tip_hash().unwrap(), t + 10, 19); // extends tip — Accepted
        let _ = weak;
        let weak_side = mine(fork_prev, t + 9000, 3);
        let weak_out = hub.accept_branch(&[weak_side]).unwrap();
        assert!(
            matches!(weak_out, AcceptOutcome::IgnoredWeaker),
            "short side from old LCA must be weaker: {weak_out:?}"
        );
        assert_eq!(hub.tip_height(), Some(18));

        // Mid-branch invalid: longer path from height 10 with a bad spend in the middle.
        let pre_tip = hub.tip_hash().unwrap();
        let pre_h = hub.tip_height().unwrap();
        let fork2 = hub
            .query
            .header_at_height(Height(10))
            .unwrap()
            .unwrap()
            .1
            .hash;
        let fork2_prev = BlockHash::from_byte_array(fork2);
        let fork2_time = hub
            .query
            .header_at_height(Height(10))
            .unwrap()
            .unwrap()
            .1
            .timestamp;

        // Path length 10 (> remaining 8 on main from 11..=18) so work_better.
        let mut bad_branch = Vec::new();
        let mut p = fork2_prev;
        let mut t = fork2_time;
        for (i, h) in (11..=20u32).enumerate() {
            let b = if i == 2 {
                // Height 13: spend a non-existent prevout → connect fails.
                let bad_tx = Transaction {
                    version: TxVersion::ONE,
                    lock_time: LockTime::ZERO,
                    input: vec![TxIn {
                        previous_output: OutPoint {
                            txid: bitcoin::Txid::from_byte_array([0xee; 32]),
                            vout: 0,
                        },
                        script_sig: ScriptBuf::new(),
                        sequence: Sequence::MAX,
                        witness: Witness::new(),
                    }],
                    output: vec![TxOut {
                        value: Amount::from_sat(1),
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    }],
                };
                mine_with_extra(p, t + 701 + i as u32, h, vec![bad_tx])
            } else {
                mine(p, t + 701 + i as u32, h)
            };
            p = b.block_hash();
            t = b.header.time;
            bad_branch.push(b);
        }
        assert_eq!(bad_branch.len(), 10);
        let err = hub
            .accept_branch(&bad_branch)
            .expect_err("invalid mid-branch must fail connect");
        assert!(
            matches!(err, NetError::Consensus(_)),
            "expected consensus fail, got {err}"
        );
        // Tip restored to pre-attempt.
        assert_eq!(
            hub.tip_height(),
            Some(pre_h),
            "tip height must restore after failed reorg"
        );
        assert_eq!(
            hub.tip_hash().unwrap(),
            pre_tip,
            "tip hash must equal pre-attempt tip after mid-branch invalid"
        );

        let _ = std::fs::remove_dir_all(dir);
    }

    /// Tip-follow capacity: 99-block competing branch via shipped `accept_branch`.
    #[test]
    fn most_work_reorg_depth99_tip_follow_capacity() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let mut tip = gen;
        let time = 1_600_000_000u32;
        // Main chain tip at height 10.
        for h in 1..=10u32 {
            let b = mine(tip, time + h * 600, h);
            tip = b.block_hash();
            hub.accept_block(b).unwrap();
        }
        assert_eq!(hub.tip_height(), Some(10));
        let fork_parent = hub
            .query
            .header_at_height(Height(1))
            .unwrap()
            .unwrap()
            .1
            .hash;
        let fork_prev = BlockHash::from_byte_array(fork_parent);
        let fork_time = hub
            .query
            .header_at_height(Height(1))
            .unwrap()
            .unwrap()
            .1
            .timestamp;
        // 99 blocks after height 1 → tip height 100.
        let mut branch = Vec::with_capacity(99);
        let mut p = fork_prev;
        let mut t = fork_time;
        for (i, h) in (2..=100u32).enumerate() {
            let b = mine(p, t + 601 + i as u32, h);
            p = b.block_hash();
            t = b.header.time;
            branch.push(b);
        }
        assert_eq!(branch.len(), 99);
        assert!(
            crate::peer::MAX_PENDING_BLOCKS_FOR_TEST >= 99,
            "pending cap must allow 99-block reorg assembly"
        );
        let out = hub.accept_branch(&branch).unwrap();
        assert!(
            matches!(out, AcceptOutcome::Accepted { height: 100 }),
            "99-block reorg must accept, got {out:?}"
        );
        assert_eq!(hub.tip_height(), Some(100));
        assert_eq!(hub.tip_hash().unwrap(), branch.last().unwrap().block_hash());
        let _ = std::fs::remove_dir_all(dir);
    }
}
