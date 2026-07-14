//! Shared chain accept path for P2P: tip extension and most-work reorg.

use crate::cache::BlockCache;
use crate::error::NetError;
use bitcoin::block::Header;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash, Work};
use rbitcoin_consensus::{accept_and_connect_block, ChainParams, Milestone};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
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
}

impl ChainHub {
    pub fn new(query: Query, params: ChainParams, milestone: Milestone) -> Self {
        let (tip_tx, _) = broadcast::channel(64);
        Self {
            query: Arc::new(query),
            cache: Arc::new(BlockCache::new()),
            params,
            milestone,
            notify: Arc::new(Notify::new()),
            tip_tx,
        }
    }

    pub fn subscribe_tips(&self) -> broadcast::Receiver<TipEvent> {
        self.tip_tx.subscribe()
    }

    pub fn tip_height(&self) -> Option<u32> {
        self.query
            .tip_height()
            .map(|h| h.0)
            .or_else(|| self.cache.tip_height())
    }

    pub fn tip_hash(&self) -> Option<BlockHash> {
        self.query
            .tip_height()
            .and_then(|h| self.query.header_at_height(h).ok().flatten())
            .map(|(_, rec)| BlockHash::from_byte_array(rec.hash))
            .or_else(|| self.cache.tip_hash())
    }

    pub fn tip_header(&self) -> Option<Header> {
        let h = self.tip_height()?;
        self.query.wire_header_at_height(Height(h)).ok()
    }

    pub fn has_block(&self, hash: &BlockHash) -> bool {
        if self.cache.get_block(hash).is_some() {
            return true;
        }
        // O(1) header hash-head lookup (avoids full confirmed scan during IBD).
        self.query
            .get_header_by_hash(&hash.to_byte_array())
            .ok()
            .flatten()
            .is_some()
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
                self.query
                    .disconnect_tip()
                    .map_err(|e| NetError::Consensus(e.to_string()))?;
            }
            self.cache.clear();
        }

        let base = fork_height.map(|h| h + 1).unwrap_or(0);
        for (i, b) in blocks.iter().enumerate() {
            self.connect_at(base + i as u32, b.clone())?;
        }
        let height = base + (blocks.len() as u32) - 1;
        Ok(AcceptOutcome::Accepted { height })
    }

    fn connect_at(&self, height: u32, block: Block) -> Result<(), NetError> {
        accept_and_connect_block(
            &self.query,
            &self.params,
            Height(height),
            &block,
            self.milestone,
        )
        .map_err(|e| NetError::Consensus(e.to_string()))?;
        let _ = self.cache.push_best(block.clone());
        let event = TipEvent {
            height,
            hash: block.block_hash(),
            header: block.header,
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
