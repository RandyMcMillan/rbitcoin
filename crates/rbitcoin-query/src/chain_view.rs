//! Header navigation, locators, height lookup.

use super::*;

impl Query {
    pub fn header_at_height(
        &self,
        height: Height,
    ) -> Result<Option<(Fk, HeaderRecord)>, QueryError> {
        match self.store.confirmed.get(height)? {
            None => Ok(None),
            Some(fk) => Ok(Some((fk, self.store.get_header(fk)?))),
        }
    }

    /// Best-chain height of a header hash, if it is **confirmed** on the tip chain.
    ///
    /// Archive may contain orphan header rows (partial connect failures). Those are
    /// not reported here — only hashes reachable as `confirmed[height]`.
    pub fn height_of_hash(&self, hash: &[u8; 32]) -> Result<Option<Height>, QueryError> {
        let Some(tip) = self.tip_height() else {
            // Only genesis can be "confirmed" with no tip.
            return Ok(None);
        };
        // Fast path: tip
        if let Some((tip_fk, rec)) = self.header_at_height(tip)? {
            if &rec.hash == hash {
                return Ok(Some(tip));
            }
            // Fast path: tip-1 (common parent checks)
            if tip.0 > 0 {
                if let Some((_, prec)) = self.header_at_height(Height(tip.0 - 1))? {
                    if &prec.hash == hash {
                        return Ok(Some(Height(tip.0 - 1)));
                    }
                }
            }
            let _ = tip_fk;
        }
        // Must appear in archive at all.
        let Some((fk, _rec)) = self.get_header_by_hash(hash)? else {
            return Ok(None);
        };
        // Confirm it is the header at some best-chain height by walking tip→genesis
        // via confirmed table only (not the orphaned archive row).
        // Prefer short reverse scan from tip (IBD / locator hot path).
        const RECENT: u32 = 4096;
        let start = tip.0.saturating_sub(RECENT);
        for h in (start..=tip.0).rev() {
            let height = Height(h);
            if let Some((hfk, rec)) = self.header_at_height(height)? {
                if hfk == fk || &rec.hash == hash {
                    return Ok(Some(height));
                }
            }
        }
        // Full scan only if not in recent window (rare for IBD).
        if start > 0 {
            for h in (0..start).rev() {
                let height = Height(h);
                if let Some((hfk, rec)) = self.header_at_height(height)? {
                    if hfk == fk || &rec.hash == hash {
                        return Ok(Some(height));
                    }
                }
            }
        }
        // Present in archive but not on best chain (orphan header row).
        Ok(None)
    }

    /// Wire header for a confirmed height (resolves prev hash from archive).
    pub fn wire_header_at_height(&self, height: Height) -> Result<BlockHeader, QueryError> {
        let (_fk, rec) = self
            .header_at_height(height)?
            .ok_or(StoreError::NotFound)?;
        self.wire_header_from_record(&rec)
    }

    pub(crate) fn wire_header_from_record(&self, rec: &HeaderRecord) -> Result<BlockHeader, QueryError> {
        let prev_blockhash = if rec.prev_fk.is_null() {
            BlockHash::from_byte_array([0u8; 32])
        } else {
            let prev = self.store.get_header(rec.prev_fk)?;
            BlockHash::from_byte_array(prev.hash)
        };
        Ok(wire_header(rec, prev_blockhash))
    }

    /// Reconstruct a full wire block from Class A archive by header hash
    /// (confirmed or not). Requires `header_txs` body.
    pub fn locator_hashes(&self) -> Result<Vec<BlockHash>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(vec![BlockHash::from_byte_array([0u8; 32])]);
        };
        let mut out = Vec::new();
        let mut h = tip.0 as i64;
        let mut step = 1i64;
        while h >= 0 {
            let (_fk, rec) = self
                .header_at_height(Height(h as u32))?
                .ok_or(StoreError::NotFound)?;
            out.push(BlockHash::from_byte_array(rec.hash));
            if out.len() >= 10 {
                step *= 2;
            }
            h -= step;
        }
        // Always include genesis.
        if let Some((_fk, rec)) = self.header_at_height(Height::GENESIS)? {
            let g = BlockHash::from_byte_array(rec.hash);
            if out.last() != Some(&g) {
                out.push(g);
            }
        }
        Ok(out)
    }

    /// Headers on the best chain after the first matching locator entry, up to `limit` (max 2000).
    pub fn headers_after_locator(
        &self,
        locator: &[BlockHash],
        stop: BlockHash,
        limit: usize,
    ) -> Result<Vec<BlockHeader>, QueryError> {
        let Some(tip) = self.tip_height() else {
            return Ok(Vec::new());
        };
        let limit = limit.min(2000);
        let mut start = 0u32;
        'outer: for loc in locator {
            if loc.to_byte_array() == [0u8; 32] {
                start = 0;
                break;
            }
            // Find height of locator on our chain.
            if let Some(h) = self.height_of_hash(&loc.to_byte_array())? {
                start = h.0.saturating_add(1);
                break 'outer;
            }
        }
        // If no locator matched, Bitcoin peers typically start from genesis; we start at 0.
        let mut out = Vec::new();
        let mut h = start;
        while h <= tip.0 && out.len() < limit {
            let hdr = self.wire_header_at_height(Height(h))?;
            let hash = hdr.block_hash();
            out.push(hdr);
            if hash == stop && stop.to_byte_array() != [0u8; 32] {
                break;
            }
            h += 1;
        }
        Ok(out)
    }
}
