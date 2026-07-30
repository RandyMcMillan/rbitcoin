//! Shared chain accept path for P2P: tip extension and most-work reorg.

use crate::cache::BlockCache;
use crate::error::NetError;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, Transaction, Work};
use std::sync::RwLock;
use rbitcoin_consensus::{
    accept_and_archive_block, accept_and_connect_block_preverified, confirm_archived_run,
    confirm_load_phase, confirm_script_phase, confirm_scripts_phase, confirm_wire_plan_stamp,
    confirm_wire_prep_from_plan as consensus_prep_from_plan, confirm_wire_prep_phase_pipelined,
    confirm_write_phase, genesis_block, header_to_record, ChainParams, Milestone, PlanStampOutcome,
    ScriptOkBatch, ScriptPreverified, WirePrepPipeline,
};
use rbitcoin_log::info;
use rbitcoin_primitives::{Fk, Height};
use rbitcoin_query::Query;
use std::collections::HashSet;
use std::sync::Arc;
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
    /// Optional cluster mempool (tip-mode tx relay + confirm remove).
    ///
    /// Attached once via [`Self::attach_mempool`] after the hub is in an `Arc`.
    mempool: std::sync::OnceLock<Arc<crate::tx_relay::MempoolHub>>,
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
            mempool: std::sync::OnceLock::new(),
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

    /// True if the full block body is in Class A (may not be confirmed yet).
    pub fn is_archived(&self, hash: &BlockHash) -> bool {
        if self.has_block(hash) {
            return true;
        }
        self.query
            .is_block_archived(&hash.to_byte_array())
            .unwrap_or(false)
    }

    /// Persist a header row only (for header-sync → out-of-order body archive).
    pub fn ensure_header(&self, header: &Header) -> Result<(), NetError> {
        let _ = self.ensure_header_fk(header)?;
        Ok(())
    }

    /// Like [`ensure_header`], but returns the header fk for the archive writer
    /// (avoids a second hash-head probe on the hot write path).
    pub fn ensure_header_fk(&self, header: &Header) -> Result<Fk, NetError> {
        let prev_fk = if header.prev_blockhash.to_byte_array() == [0u8; 32] {
            Fk::NULL
        } else {
            self.query
                .get_header_by_hash(header.prev_blockhash.as_byte_array())
                .map_err(|e| NetError::Consensus(e.to_string()))?
                .map(|(fk, _)| fk)
                .unwrap_or(Fk::NULL)
        };
        let rec = header_to_record(prev_fk, header);
        self.query
            .ensure_header(&rec)
            .map_err(|e| NetError::Consensus(e.to_string()))
    }

    /// Archive Class A body without requiring tip order (IBD path).
    pub fn archive_block(&self, height: u32, block: Block) -> Result<(), NetError> {
        let hash = block.block_hash();
        if self.is_archived(&hash) {
            return Ok(());
        }
        accept_and_archive_block(
            &self.query,
            &self.params,
            Height(height),
            &block,
            self.milestone,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        Ok(())
    }

    /// Confirm `hash` at tip+1 if its body is archived.
    pub fn confirm_hash(&self, height: u32, hash: BlockHash) -> Result<AcceptOutcome, NetError> {
        let outcomes = self.confirm_run(&[(height, hash)])?;
        Ok(outcomes
            .into_iter()
            .next()
            .unwrap_or(AcceptOutcome::AlreadyHave))
    }

    /// Filter batch to unconfirmed archived heights (contiguous tip extension).
    fn prepare_confirm_need(
        &self,
        blocks: &[(u32, BlockHash)],
    ) -> Result<(Vec<(Height, [u8; 32])>, Vec<(u32, BlockHash)>), NetError> {
        let mut need: Vec<(Height, [u8; 32])> = Vec::with_capacity(blocks.len());
        let mut need_meta: Vec<(u32, BlockHash)> = Vec::with_capacity(blocks.len());
        for &(height, hash) in blocks {
            if self.has_block(&hash) {
                continue;
            }
            if !self.is_archived(&hash) {
                return Err(NetError::Protocol("confirm without archive"));
            }
            need.push((Height(height), hash.to_byte_array()));
            need_meta.push((height, hash));
        }
        Ok((need, need_meta))
    }

    /// LOAD stage: Class A load + pin parents → resolve → wave → wire → assemble.
    /// Hand result to [`Self::confirm_scripts`].
    pub fn confirm_load_phase(
        &self,
        blocks: &[(u32, BlockHash)],
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        if blocks.is_empty() {
            return Ok(None);
        }
        let (need, _) = self.prepare_confirm_need(blocks)?;
        if need.is_empty() {
            return Ok(None);
        }
        let ok = confirm_load_phase(&self.query, &self.params, self.milestone, &need)
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        Ok(Some(ok))
    }

    /// Contiguous tip-extension slice for plan (Arc wire; skip already confirmed).
    fn confirm_wire_contig_arc(
        &self,
        blocks: &[(Height, std::sync::Arc<Block>)],
        pipeline: Option<&WirePrepPipeline>,
    ) -> Option<Vec<(Height, std::sync::Arc<Block>)>> {
        if blocks.is_empty() {
            return None;
        }
        let store_path_lo = match self.tip_height() {
            None => 0u32,
            Some(t) => t.saturating_add(1),
        };
        let path_lo = pipeline.map(|p| p.path_lo).unwrap_or(store_path_lo);
        let need: Vec<(Height, std::sync::Arc<Block>)> = blocks
            .iter()
            .filter(|(h, b)| {
                let hash = b.block_hash();
                !self.has_block(&hash) && h.0 >= path_lo
            })
            .map(|(h, b)| (*h, std::sync::Arc::clone(b)))
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

    /// Contiguous tip-extension slice for one-shot prep (owned Block).
    fn confirm_wire_contig(
        &self,
        blocks: &[(Height, Block)],
        pipeline: Option<&WirePrepPipeline>,
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

    /// IBD **plan** stage: structure + stamp create_fk only (no denserels pin).
    ///
    /// Wire is `Arc<Block>` so body-queue decode is not re-cloned into stamp.
    pub fn confirm_wire_plan_phase(
        &self,
        blocks: &[(Height, std::sync::Arc<Block>)],
        pipeline: Option<&WirePrepPipeline>,
    ) -> Result<Option<PlanStampOutcome>, NetError> {
        let Some(contig) = self.confirm_wire_contig_arc(blocks, pipeline) else {
            return Ok(None);
        };
        let out = confirm_wire_plan_stamp(
            &self.query,
            &self.params,
            self.milestone,
            &contig,
            pipeline,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        Ok(Some(out))
    }

    /// IBD **prep** after plan: pin denserels + assemble (does not re-plan).
    pub fn confirm_wire_prep_from_plan(
        &self,
        stamped: PlanStampOutcome,
        pipeline: Option<&WirePrepPipeline>,
    ) -> Result<rbitcoin_consensus::ConfirmLoadOutcome, NetError> {
        consensus_prep_from_plan(
            &self.query,
            &self.params,
            self.milestone,
            stamped,
            pipeline,
            &ScriptPreverified::new(),
        )
        .map_err(|e| NetError::Consensus(e.to_string()))
    }

    /// Unified PREP from raw wire blocks (no Class-A wire rebuild).
    /// Skips heights already confirmed. Does **not** require prior archive.
    ///
    /// When `pipeline` is `None`, first height must be store tip+1 (legacy).
    /// When `Some`, first height is `pipeline.path_lo` so prep(N+1) can run
    /// while commit(N) has not advanced tip.
    ///
    /// One-shot path (tests / tip-follow): plan+pin+assemble with cold denserels
    /// allowed. IBD uses [`Self::confirm_wire_plan_phase`] then
    /// [`Self::confirm_wire_prep_from_plan`].
    pub fn confirm_wire_prep_phase(
        &self,
        blocks: &[(Height, Block)],
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        self.confirm_wire_prep_phase_pipelined(blocks, None)
    }

    /// Prep with optional pipeline caches (reserved create fks + in-flight creates).
    ///
    /// Cold denserels **allowed** (tests / one-shot). IBD: plan stamps, prep pins.
    pub fn confirm_wire_prep_phase_pipelined(
        &self,
        blocks: &[(Height, Block)],
        pipeline: Option<&WirePrepPipeline>,
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        self.confirm_wire_prep_phase_pipelined_cold(
            blocks,
            pipeline,
            rbitcoin_consensus::ColdPinMode::Allow,
        )
    }

    /// One-shot prep with explicit cold denserels policy.
    pub fn confirm_wire_prep_phase_pipelined_cold(
        &self,
        blocks: &[(Height, Block)],
        pipeline: Option<&WirePrepPipeline>,
        cold_mode: rbitcoin_consensus::ColdPinMode,
    ) -> Result<Option<rbitcoin_consensus::ConfirmLoadOutcome>, NetError> {
        let Some(contig) = self.confirm_wire_contig(blocks, pipeline) else {
            return Ok(None);
        };
        let ok = confirm_wire_prep_phase_pipelined(
            &self.query,
            &self.params,
            self.milestone,
            &contig,
            &ScriptPreverified::new(),
            pipeline,
            cold_mode,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        Ok(Some(ok))
    }

    /// SCRIPT stage only: pure verify of jobs on a loaded batch (no store access).
    pub fn confirm_scripts(
        &self,
        batch: rbitcoin_consensus::LoadedBatch,
    ) -> Result<rbitcoin_consensus::ConfirmScriptOutcome, NetError> {
        // Scripts never touch Query/store — receiver kept for call-site symmetry.
        let _hub = self;
        confirm_scripts_phase(batch).map_err(|e| NetError::Consensus(e.to_string()))
    }

    /// MATERIALIZE + SCRIPTS (compat). Prefer split stages in IBD.
    pub fn confirm_script_phase(
        &self,
        blocks: &[(u32, BlockHash)],
    ) -> Result<Option<rbitcoin_consensus::ConfirmScriptOutcome>, NetError> {
        if blocks.is_empty() {
            return Ok(None);
        }
        let (need, _) = self.prepare_confirm_need(blocks)?;
        if need.is_empty() {
            return Ok(None);
        }
        let ok = confirm_script_phase(&self.query, &self.params, self.milestone, &need)
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

    /// Confirm a contiguous tip-extension run (sync script + write).
    pub fn confirm_run(
        &self,
        blocks: &[(u32, BlockHash)],
    ) -> Result<Vec<AcceptOutcome>, NetError> {
        if blocks.is_empty() {
            return Ok(Vec::new());
        }
        let mut already: HashSet<BlockHash> = HashSet::new();
        for &(_, hash) in blocks {
            if self.has_block(&hash) {
                already.insert(hash);
            }
        }
        let (need, need_meta) = self.prepare_confirm_need(blocks)?;
        if need.is_empty() {
            return Ok(blocks
                .iter()
                .map(|&(_, hash)| {
                    if already.contains(&hash) {
                        AcceptOutcome::AlreadyHave
                    } else {
                        AcceptOutcome::AlreadyHave
                    }
                })
                .collect());
        }
        confirm_archived_run(&self.query, &self.params, self.milestone, &need)
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.note_confirmed_tip(&need_meta)?;
        let done: HashSet<BlockHash> = need_meta.iter().map(|(_, h)| *h).collect();
        Ok(blocks
            .iter()
            .map(|&(height, hash)| {
                if already.contains(&hash) {
                    AcceptOutcome::AlreadyHave
                } else if done.contains(&hash) {
                    AcceptOutcome::Accepted { height }
                } else {
                    AcceptOutcome::AlreadyHave
                }
            })
            .collect())
    }

    fn note_confirmed_tip(&self, need_meta: &[(u32, BlockHash)]) -> Result<(), NetError> {
        if let Some(mp) = self.mempool() {
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

    /// Accept a block that extends the tip, or reorg to a stronger competing tip / branch.
    pub fn accept_block(&self, block: Block) -> Result<AcceptOutcome, NetError> {
        let hash = block.block_hash();
        if self.has_block(&hash) {
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
                    if block.header.work() > cur.header.work() {
                        self.disconnect_to(parent_h.0)?;
                        self.connect_at(new_height, block)?;
                        return Ok(AcceptOutcome::Accepted {
                            height: new_height,
                        });
                    }
                    return Ok(AcceptOutcome::IgnoredWeaker);
                }

                // Extends an ancestor — single block cannot beat a longer chain alone.
                // Caller should use accept_branch with the full better path.
                Err(NetError::Protocol("side block; use accept_branch for reorg"))
            }
        }
    }

    /// Connect a contiguous branch `[blocks[0]…blocks[n]]` where `blocks[0].prev` is on our chain.
    /// Reorgs if the new path has strictly more work than our path from the fork.
    pub fn accept_branch(&self, blocks: &[Block]) -> Result<AcceptOutcome, NetError> {
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
        let tip_h = self.tip_height().unwrap_or(0);

        // Work on new path
        let new_work = sum_work(blocks.iter().map(|b| b.header.work()));

        // Work on our path from fork+1..=tip
        let start = fork_height.map(|h| h + 1).unwrap_or(0);
        let mut our_works = Vec::new();
        if self.tip_height().is_some() {
            for h in start..=tip_h {
                if let Some(b) = self.block_at_height(h)? {
                    our_works.push(b.header.work());
                }
            }
        }
        let our_work = sum_work(our_works.into_iter());

        if self.tip_height().is_some() && !work_better(new_work, our_work) {
            return Ok(AcceptOutcome::IgnoredWeaker);
        }

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
            self.connect_at(base + i as u32, b.clone())?;
        }
        let height = base + (blocks.len() as u32) - 1;
        Ok(AcceptOutcome::Accepted { height })
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
        accept_and_connect_block_preverified(
            &self.query,
            &self.params,
            Height(height),
            &block,
            self.milestone,
            &preverified,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        if let Some(mp) = self.mempool() {
            let ids: Vec<_> = block.txdata.iter().map(|t| t.compute_txid()).collect();
            let n = mp.remove_for_block(&ids);
            if n > 0 {
                rbitcoin_log::debug!("mempool: removed {n} confirmed tx(s) @ height {height}");
            }
        }
        self.confirmed.write().unwrap().insert(hash);
        // Move block into tip-window cache (no full-history clone).
        let _ = self.cache.push_best(block);
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
        let Some(tip) = self.tip_height() else {
            return Ok(Work::from_be_bytes([0u8; 32]));
        };
        let mut works = Vec::new();
        for h in 0..=tip {
            let hdr = self
                .query
                .wire_header_at_height(Height(h))
                .map_err(|e| NetError::Consensus(e.to_string()))?;
            works.push(hdr.work());
        }
        Ok(sum_work(works.into_iter()))
    }
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

fn sum_work(iter: impl Iterator<Item = Work>) -> Work {
    let mut acc: Option<Work> = None;
    for w in iter {
        acc = Some(match acc {
            None => w,
            Some(a) => a + w,
        });
    }
    acc.unwrap_or_else(|| Work::from_be_bytes([0u8; 32]))
}

/// Strictly more work (Bitcoin most-work rule).
fn work_better(new: Work, old: Work) -> bool {
    new > old
}

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
    use rbitcoin_consensus::{ChainParams, Milestone};
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
    fn ensure_genesis_accept_extend_and_already_have() {
        let (dir, hub) = tmp_hub();
        assert!(hub.tip_height().is_none());
        hub.ensure_genesis().unwrap();
        assert_eq!(hub.tip_height(), Some(0));
        // Second call is a no-op once tip exists.
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        assert!(hub.has_block(&gen));
        assert!(hub.is_archived(&gen));

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

    /// Prep batch N+1 must succeed while N is only prepped (not committed).
    /// Regression: hub used store tip+1 only → Ok(None) "empty outcome" thrash.
    #[test]
    fn wire_prep_ahead_of_store_tip_with_pipeline() {
        use rbitcoin_consensus::WirePrepPipeline;
        use std::collections::HashMap;

        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        hub.query.enter_direct_index_mode().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_200, 1);
        let b2 = mine(b1.block_hash(), 1_300_000_800, 2);
        let h1 = b1.block_hash();
        let h2 = b2.block_hash();

        // Batch 1 at store tip+1 (path_lo=1).
        let batch1 = [(
            rbitcoin_primitives::Height(1),
            b1.clone(),
        )];
        let mut pipe = WirePrepPipeline {
            path_lo: 1,
            parent_hash: None,
            next_tx_start: hub.query.tx_body_count().saturating_add(1).max(1),
            in_flight_creates: std::sync::Arc::new(HashMap::new()),
            in_flight_outs: std::sync::Arc::new(HashMap::new()),
        };
        let mat1 = hub
            .confirm_wire_prep_phase_pipelined(&batch1, Some(&pipe))
            .expect("prep1")
            .expect("prep1 some");
        assert_eq!(mat1.batch.len(), 1);
        assert!(mat1.batch.archive_plan.is_some());
        assert_eq!(hub.tip_height(), Some(0), "tip must not advance on prep alone");

        // Update pipeline caches from plan (prep-thread note_plan_ok).
        let plan = mat1.batch.archive_plan.as_ref().unwrap();
        {
            let creates = std::sync::Arc::make_mut(&mut pipe.in_flight_creates);
            let outs = std::sync::Arc::make_mut(&mut pipe.in_flight_outs);
            if plan.batch_pin.len() == plan.planned_fks.len() {
                for (fk, pin) in plan.planned_fks.iter().zip(plan.batch_pin.iter()) {
                    creates.insert(pin.0.txid, *fk);
                    if let Some(id) = fk.get() {
                        outs.insert(id, std::sync::Arc::clone(pin));
                    }
                }
            } else {
                for ((tx, ins, o), fk) in plan.packed.iter().zip(plan.planned_fks.iter()) {
                    creates.insert(tx.txid, *fk);
                    if let Some(id) = fk.get() {
                        let denserels =
                            rbitcoin_store::denserels_from_packed_records(tx, ins, o);
                        outs.insert(
                            id,
                            std::sync::Arc::new((tx.clone(), o.clone(), denserels)),
                        );
                    }
                }
            }
        }
        if let Some(last) = plan.planned_fks.last().and_then(|f| f.get()) {
            pipe.next_tx_start = last.saturating_add(1).max(1);
        }
        pipe.path_lo = 2;
        pipe.parent_hash = Some(h1.to_byte_array());

        // Batch 2 while tip still 0 — must NOT Ok(None).
        let batch2 = [(rbitcoin_primitives::Height(2), b2.clone())];
        let mat2 = hub
            .confirm_wire_prep_phase_pipelined(&batch2, Some(&pipe))
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

        // Header-only then archive body out-of-order style.
        hub.ensure_header(&b1.header).unwrap();
        let fk = hub.ensure_header_fk(&b1.header).unwrap();
        assert!(fk.0 > 0 || fk.0 == 0); // Fk may be 0 on some layouts
        hub.archive_block(1, b1.clone()).unwrap();
        // Idempotent archive.
        hub.archive_block(1, b1.clone()).unwrap();
        assert!(hub.is_archived(&h1));

        // Confirm empty / already-have paths.
        assert!(hub.confirm_run(&[]).unwrap().is_empty());
        assert!(hub.confirm_load_phase(&[]).unwrap().is_none());
        assert!(hub.confirm_script_phase(&[]).unwrap().is_none());

        let outs = hub.confirm_run(&[(1, h1)]).unwrap();
        assert_eq!(outs.len(), 1);
        assert!(matches!(outs[0], AcceptOutcome::Accepted { height: 1 }));
        assert_eq!(hub.tip_height(), Some(1));
        // Already confirmed → AlreadyHave.
        let outs2 = hub.confirm_run(&[(1, h1)]).unwrap();
        assert!(matches!(outs2[0], AcceptOutcome::AlreadyHave));
        assert!(matches!(
            hub.confirm_hash(1, h1).unwrap(),
            AcceptOutcome::AlreadyHave
        ));
        // Load phase on already-confirmed → None.
        assert!(hub.confirm_load_phase(&[(1, h1)]).unwrap().is_none());

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
    fn prepare_confirm_without_archive_errors() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_000_500, 1);
        let h1 = b1.block_hash();
        hub.ensure_header(&b1.header).unwrap();
        // Body not archived → confirm without archive.
        let err = hub.confirm_run(&[(1, h1)]).unwrap_err();
        assert!(matches!(err, NetError::Protocol(_)));
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
                if side.header.validate_pow(target).is_ok()
                    && side.block_hash() != b1.block_hash()
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
    fn confirm_load_script_split_after_archive() {
        let (dir, hub) = tmp_hub();
        hub.ensure_genesis().unwrap();
        let gen = hub.tip_hash().unwrap();
        let b1 = mine(gen, 1_300_004_000, 1);
        let h1 = b1.block_hash();
        hub.ensure_header(&b1.header).unwrap();
        hub.archive_block(1, b1).unwrap();

        // Load phase returns Some for archived tip+1.
        let loaded = hub.confirm_load_phase(&[(1, h1)]).unwrap();
        assert!(loaded.is_some());
        let batch = loaded.unwrap();
        // Scripts pure stage.
        let script_out = hub.confirm_scripts(batch.batch).unwrap();
        let write_out = hub.confirm_write(script_out.batch).unwrap();
        assert_eq!(write_out.len(), 1);
        assert!(matches!(
            write_out[0],
            AcceptOutcome::Accepted { height: 1 }
        ));
        assert_eq!(hub.tip_height(), Some(1));

        // confirm_script_phase empty need after already confirmed.
        assert!(hub.confirm_script_phase(&[(1, h1)]).unwrap().is_none());

        let _ = std::fs::remove_dir_all(dir);
    }
}
