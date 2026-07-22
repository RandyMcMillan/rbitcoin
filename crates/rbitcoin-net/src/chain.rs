//! Shared chain accept path for P2P: tip extension and most-work reorg.

use crate::cache::BlockCache;
use crate::error::NetError;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, Work};
use std::sync::RwLock;
use rbitcoin_consensus::{
    accept_and_archive_block, accept_and_connect_block, confirm_archived_run,
    confirm_script_phase, confirm_writeback_phase, genesis_block, header_to_record,
    ChainParams, Milestone, ScriptOkBatch,
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

    /// SCRIPT stage only (optimistic). Hand result to [`Self::confirm_writeback`].
    pub fn confirm_script_phase(
        &self,
        blocks: &[(u32, BlockHash)],
    ) -> Result<Option<ScriptOkBatch>, NetError> {
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

    /// WRITEBACK stage: structural + Class C + spend annotate (ordered).
    pub fn confirm_writeback(&self, batch: ScriptOkBatch) -> Result<Vec<AcceptOutcome>, NetError> {
        let meta: Vec<(u32, BlockHash)> = batch
            .heights_hashes()
            .into_iter()
            .map(|(h, raw)| (h, BlockHash::from_byte_array(raw)))
            .collect();
        confirm_writeback_phase(&self.query, &self.params, self.milestone, batch)
            .map_err(|e| NetError::Consensus(e.to_string()))?;
        self.note_confirmed_tip(&meta)?;
        Ok(meta
            .iter()
            .map(|&(height, _)| AcceptOutcome::Accepted { height })
            .collect())
    }

    /// Confirm a contiguous tip-extension run (sync script + writeback).
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
        accept_and_connect_block(
            &self.query,
            &self.params,
            Height(height),
            &block,
            self.milestone,
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
        loop {
            let tip = match self.query.tip_height() {
                Some(h) => h.0,
                None => break,
            };
            if tip <= keep_height {
                break;
            }
            if let Some(th) = self.tip_hash() {
                self.confirmed.write().unwrap().remove(&th);
            }
            self.query
                .disconnect_tip()
                .map_err(|e| NetError::Consensus(e.to_string()))?;
        }
        self.cache.truncate_to_height(keep_height);
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
