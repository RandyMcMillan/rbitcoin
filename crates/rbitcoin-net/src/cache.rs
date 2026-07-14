//! In-memory wire block cache for P2P serve and ordered sync.

use bitcoin::block::{Block, Header};
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct BlockCache {
    inner: RwLock<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    by_hash: HashMap<BlockHash, Block>,
    /// Best-chain hashes by height (contiguous from 0).
    chain: Vec<BlockHash>,
}

impl BlockCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn tip_height(&self) -> Option<u32> {
        let g = self.inner.read();
        if g.chain.is_empty() {
            None
        } else {
            Some((g.chain.len() - 1) as u32)
        }
    }

    pub fn tip_hash(&self) -> Option<BlockHash> {
        self.inner.read().chain.last().copied()
    }

    pub fn len(&self) -> usize {
        self.inner.read().chain.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().chain.is_empty()
    }

    pub fn get_block(&self, hash: &BlockHash) -> Option<Block> {
        self.inner.read().by_hash.get(hash).cloned()
    }

    pub fn get_header(&self, hash: &BlockHash) -> Option<Header> {
        self.inner.read().by_hash.get(hash).map(|b| b.header)
    }

    pub fn hash_at_height(&self, height: u32) -> Option<BlockHash> {
        self.inner.read().chain.get(height as usize).copied()
    }

    pub fn header_at_height(&self, height: u32) -> Option<Header> {
        let g = self.inner.read();
        let h = g.chain.get(height as usize)?;
        g.by_hash.get(h).map(|b| b.header)
    }

    /// Append a block that extends the best chain (or becomes genesis at height 0).
    pub fn push_best(&self, block: Block) -> Result<(), &'static str> {
        let hash = block.block_hash();
        let mut g = self.inner.write();
        if g.chain.is_empty() {
            if block.header.prev_blockhash != BlockHash::from_byte_array([0u8; 32])
                && block.header.prev_blockhash.to_byte_array() != [0u8; 32]
            {
                // regtest genesis has zero prev — ok
            }
            g.by_hash.insert(hash, block);
            g.chain.push(hash);
            return Ok(());
        }
        let tip = *g.chain.last().unwrap();
        if block.header.prev_blockhash != tip {
            return Err("block does not extend best chain");
        }
        g.by_hash.insert(hash, block);
        g.chain.push(hash);
        Ok(())
    }

    /// Locator hashes newest-first for getheaders.
    pub fn locator(&self) -> Vec<BlockHash> {
        let g = self.inner.read();
        if g.chain.is_empty() {
            return vec![BlockHash::from_byte_array([0u8; 32])];
        }
        let mut out = Vec::new();
        let mut i = g.chain.len() as i64 - 1;
        let mut step = 1i64;
        while i >= 0 {
            out.push(g.chain[i as usize]);
            if out.len() >= 10 {
                step *= 2;
            }
            i -= step;
        }
        // Always include genesis
        if let Some(g0) = g.chain.first() {
            if out.last() != Some(g0) {
                out.push(*g0);
            }
        }
        out
    }

    /// Headers after the best common locator hash, up to 2000.
    pub fn headers_after_locator(&self, locator: &[BlockHash], stop: BlockHash) -> Vec<Header> {
        let g = self.inner.read();
        if g.chain.is_empty() {
            return Vec::new();
        }
        let mut start = 0usize;
        'outer: for loc in locator {
            if loc.to_byte_array() == [0u8; 32] {
                start = 0;
                break;
            }
            for (i, h) in g.chain.iter().enumerate() {
                if h == loc {
                    start = i + 1;
                    break 'outer;
                }
            }
        }
        let mut out = Vec::new();
        for h in g.chain.iter().skip(start).take(2000) {
            if let Some(b) = g.by_hash.get(h) {
                out.push(b.header);
                if *h == stop && stop.to_byte_array() != [0u8; 32] {
                    break;
                }
            }
        }
        out
    }
}
