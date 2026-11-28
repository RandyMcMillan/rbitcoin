//! BIP158 basic filters over [`rbitcoin_store::BlockFilterTable`].
//!
//! Additive index under the store dir; Class A is unchanged. Filters are
//! built from Class A (output scripts plus spent prevout scripts through
//! `input.body` parent edges), so heights below a seqsigwit prune still build.
//! The watermark is the last committed height.

use bitcoin::bip158::{BitStreamWriter, BlockFilter, FilterHash, FilterHeader};
use bitcoin::consensus::encode::{Encodable, VarInt};
use bitcoin::hashes::{siphash24, Hash};
use bitcoin::{Block, OutPoint};
use rbitcoin_primitives::{le_u64, Fk, Height};
use rbitcoin_store::{
    read_index_window, BlockFilterRecord, BlockFilterSlot, BlockFilterTable, IndexHeight,
    IndexWindow, StoreError,
};

use crate::{Query, QueryError};
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex};

#[cfg(debug_assertions)]
thread_local! {
    static LIE_FILTER_BODY: std::cell::RefCell<Option<Vec<u8>>> =
        const { std::cell::RefCell::new(None) };
}

/// The next `basic_filter_at` on this thread that finds a row returns `body`.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn lie_next_basic_filter_body(body: Vec<u8>) {
    LIE_FILTER_BODY.with(|slot| *slot.borrow_mut() = Some(body));
}

#[cfg(debug_assertions)]
fn take_lie_filter_body() -> Option<Vec<u8>> {
    LIE_FILTER_BODY.with(|slot| slot.borrow_mut().take())
}

#[cfg(debug_assertions)]
fn lie_filter_row(mut row: Option<(Vec<u8>, FilterHeader)>) -> Option<(Vec<u8>, FilterHeader)> {
    if let Some((body, _)) = row.as_mut() {
        if let Some(lie) = take_lie_filter_body() {
            *body = lie;
        }
    }
    row
}

/// Core `getblockfilter` when the index has not reached this block.
#[doc(hidden)]
pub const FILTER_STILL_INDEXING: &str =
    "Filter not found. Block filters are still in the process of being indexed.";

/// Stale ancestors a `getblockfilter` rebuild walks back to its best-chain
/// fork point, the same bound as Esplora's stored-header walk. Each one is
/// a block body rebuilt and filtered.
#[doc(hidden)]
pub const MAX_STALE_FILTER_WALK: usize = 10_000;

/// `getblockfilter` for a stale block whose branch is longer than
/// [`MAX_STALE_FILTER_WALK`]. Core keeps every connected block's filter
/// and has no such limit.
#[doc(hidden)]
pub const FILTER_STALE_TOO_DEEP: &str =
    "Filter not found. Stale branch is deeper than 10000 blocks.";

/// BIP158 basic filter Golomb-Rice parameters (`M`, `P`).
#[doc(hidden)]
pub const BASIC_FILTER_M: u64 = 784_931;
#[doc(hidden)]
pub const BASIC_FILTER_P: u8 = 19;
pub struct BlockFilterWriteBehind {
    /// Serializes commits against disconnect truncate.
    pub appender: Mutex<()>,
    pub wake: Mutex<()>,
    pub wake_cv: Condvar,
}

impl Default for BlockFilterWriteBehind {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockFilterWriteBehind {
    pub fn new() -> Self {
        Self {
            appender: Mutex::new(()),
            wake: Mutex::new(()),
            wake_cv: Condvar::new(),
        }
    }

    pub fn notify(&self) {
        self.wake_cv.notify_one();
    }

    pub fn lock_appender(&self) -> std::sync::MutexGuard<'_, ()> {
        self.appender.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[doc(hidden)]
    pub fn wait(&self, d: std::time::Duration) {
        let g = self.wake.lock().unwrap_or_else(|e| e.into_inner());
        let _ = self.wake_cv.wait_timeout(g, d);
    }
}

/// Basic filter of `window.blocks[i]` using a hash the caller already loaded.
///
/// No store IO. A missing prevout is corrupt.
pub fn basic_filter_of(
    block_hash: &[u8; 32],
    window: &IndexWindow,
    i: usize,
) -> Result<BlockFilter, QueryError> {
    let block = &window.blocks[i];
    let mut spent = Vec::new();
    for e in block.edges.iter().flatten().filter(|e| !e.parent.is_null()) {
        let out = window.prevout(e.parent, e.vout).ok_or(StoreError::Corrupt(
            "invariant: blockfilter prevout missing",
        ))?;
        spent.push(out.script.as_slice());
    }
    let outputs = block
        .txs
        .iter()
        .flat_map(|tx| tx.outs.iter().map(|o| o.script.as_slice()));
    basic_filter_from_scripts(block_hash, outputs, spent)
}

/// GCS-encode a basic filter. Non-`OP_RETURN` output scripts, then each spent
/// prevout script. `block_hash` is internal byte order.
pub fn basic_filter_from_scripts<'a>(
    block_hash: &[u8; 32],
    output_scripts: impl IntoIterator<Item = &'a [u8]>,
    spent_scripts: impl IntoIterator<Item = &'a [u8]>,
) -> Result<BlockFilter, QueryError> {
    let elements = output_scripts
        .into_iter()
        .filter(|script| script.first() != Some(&rbitcoin_primitives::OP_RETURN))
        .chain(spent_scripts);
    encode_basic_filter(block_hash, elements)
}

/// GCS-encode a basic filter keyed by `block_hash` (internal byte order).
///
/// The same bytes as rust-bitcoin's `GcsFilterWriter`, which copies every
/// element into an ordered set. Here elements stay borrowed: each is keyed by
/// its SipHash, sorted by (hash, bytes), and deduplicated on equal bytes. The
/// range map is monotonic in the hash, so the mapped values come out sorted.
#[doc(hidden)]
pub fn encode_basic_filter<'a>(
    block_hash: &[u8; 32],
    elements: impl Iterator<Item = &'a [u8]>,
) -> Result<BlockFilter, QueryError> {
    let k0 = le_u64!(block_hash, 0);
    let k1 = le_u64!(block_hash, 8);
    let mut keyed: Vec<(u64, &[u8])> = elements
        .filter(|e| !e.is_empty())
        .map(|e| (siphash24::Hash::hash_to_u64_with_keys(k0, k1, e), e))
        .collect();
    keyed.sort_unstable();
    keyed.dedup();
    let content = golomb_rice_set(keyed.iter().map(|(hash, _)| *hash), keyed.len())
        .map_err(|_| StoreError::Corrupt("invariant: blockfilter encode"))?;
    Ok(BlockFilter { content })
}

/// Count, then the Golomb-Rice deltas of `sorted` hashes mapped to `[0, n·M)`.
fn golomb_rice_set(
    sorted: impl Iterator<Item = u64>,
    n: usize,
) -> Result<Vec<u8>, bitcoin::io::Error> {
    let nm = u128::from(n as u64 * BASIC_FILTER_M);
    let p = BASIC_FILTER_P;
    let mut content = Vec::with_capacity(9 + n * (usize::from(p) + 2) / 8);
    VarInt::from(n).consensus_encode(&mut content)?;
    let mut bits = BitStreamWriter::new(&mut content);
    let mut last = 0u64;
    for hash in sorted {
        let mapped = ((u128::from(hash) * nm) >> 64) as u64;
        let delta = mapped - last;
        let mut q = delta >> p;
        while q > 0 {
            let run = q.min(64);
            bits.write(u64::MAX, run as u8)?;
            q -= run;
        }
        bits.write(0, 1)?;
        bits.write(delta, p)?;
        last = mapped;
    }
    bits.flush()?;
    Ok(content)
}

fn check_filter_hash(body: &[u8], slot: &BlockFilterSlot) -> Result<(), StoreError> {
    if FilterHash::hash(body).to_byte_array() != slot.filter_hash {
        return Err(StoreError::Corrupt(
            "invariant: blockfilter body does not match its index hash",
        ));
    }
    Ok(())
}

impl Query {
    /// Confirmed heights `start..=end` as reads for [`Self::read_index_window`];
    /// heights at or above `tweaks_from` also read what tweaks need.
    pub fn index_heights(
        &self,
        start: u32,
        end: u32,
        tweaks_from: Option<u32>,
    ) -> Result<Vec<IndexHeight>, QueryError> {
        (start..=end)
            .map(|h| {
                let header_fk = self
                    .store
                    .confirmed
                    .get(Height(h))?
                    .ok_or(StoreError::Corrupt("invariant: index height not confirmed"))?;
                let (first, n) = self
                    .store
                    .header_txs
                    .get_range(header_fk)?
                    .ok_or(StoreError::Corrupt("confirmed header missing body list"))?;
                Ok(IndexHeight {
                    height: Height(h),
                    header_fk,
                    first,
                    n,
                    tweaks: tweaks_from.is_some_and(|t| h >= t),
                })
            })
            .collect()
    }

    /// Read a window of blocks and the parents they spend (completion session).
    ///
    /// Prune mode does not pread `seqsigwit.body`. P2TR inputs come from the
    /// keep window: RAM, then `seqsigwit.window`.
    pub fn read_index_window(&self, heights: &[IndexHeight]) -> Result<IndexWindow, QueryError> {
        let mut window = read_index_window(&self.store.txs, heights)?;
        if self.prune_seqsigwit() {
            self.fill_pruned_tweak_inputs(&mut window)?;
        }
        Ok(window)
    }

    fn fill_pruned_tweak_inputs(&self, window: &mut IndexWindow) -> Result<(), QueryError> {
        for block in &mut window.blocks {
            let mut file_wants = Vec::new();
            // Keep this lookup. Confirm can evict the RAM row before the
            // second pass, and the spill file is still the right copy.
            let mut ram_hits = std::collections::HashMap::new();
            for tx in &block.txs {
                if !tx.need_seqsigwit {
                    continue;
                }
                if let Some(ram) = self.seqsigwit_ram_inputs(tx.fk) {
                    ram_hits.insert(tx.fk, ram);
                } else {
                    file_wants.push((tx.fk, tx.rec.input_count));
                }
            }
            let from_file = self.read_spill_rows(block.height.0, &file_wants)?;
            for tx in &mut block.txs {
                if !tx.need_seqsigwit {
                    continue;
                }
                let inputs = if let Some(ram) = ram_hits.remove(&tx.fk) {
                    ram
                } else if let Some(row) = from_file.get(&tx.fk) {
                    row.clone()
                } else {
                    return Err(self.pruned_tweak_witness_miss(block.height));
                };
                tx.inputs = Some(inputs);
            }
        }
        Ok(())
    }

    fn pruned_tweak_witness_miss(&self, height: Height) -> QueryError {
        let dropped = self.pruneheight().is_some_and(|ph| height.0 <= ph.0);
        if dropped {
            return StoreError::Layout(crate::sp_tweaks::SP_TWEAKS_PRUNE_BEFORE_TAPROOT.into());
        }
        StoreError::Corrupt("seqsigwit window miss")
    }

    /// Basic filter of `window.blocks[i]`. Reads the block hash from the header.
    pub fn basic_filter_from_window(
        &self,
        window: &IndexWindow,
        i: usize,
    ) -> Result<BlockFilter, QueryError> {
        let hash = self.store.get_header(window.blocks[i].header_fk)?.hash;
        basic_filter_of(&hash, window, i)
    }

    pub fn block_filter_enabled(&self) -> bool {
        self.block_filter_enabled.load(Ordering::SeqCst)
    }

    /// Turn the index on (opening or creating the table) or off.
    ///
    /// Open drops committed slots that are not on the best chain: above the
    /// tip, or whose `header_fk` is not `confirmed[h]` (a crash between a
    /// disconnect and its filter truncate, or a torn slot).
    pub fn set_block_filter_index(&self, enabled: bool) -> Result<(), QueryError> {
        if enabled && self.block_filters.get().is_none() {
            let table = BlockFilterTable::open_or_create(self.store.cold_files_dir())?;
            self.trim_block_filters_to_best_chain(&table)?;
            let _ = self.block_filters.set(table);
        }
        self.block_filter_enabled.store(enabled, Ordering::SeqCst);
        Ok(())
    }

    #[doc(hidden)]
    pub fn trim_block_filters_to_best_chain(
        &self,
        table: &BlockFilterTable,
    ) -> Result<(), QueryError> {
        table.truncate_through(self.tip_height())?;
        let was = table.next_height().0;
        let mut keep = was.checked_sub(1);
        while let Some(h) = keep {
            let slot = table.slot(Height(h))?.map(|s| s.header_fk);
            if slot.is_some() && slot == self.store.confirmed.get(Height(h))? {
                break;
            }
            keep = h.checked_sub(1);
        }
        if keep.map_or(0, |h| h + 1) != was {
            rbitcoin_log::warn!(
                "blockfilter: dropping slots off the best chain: keep through {keep:?} (had {was})"
            );
            table.truncate_through(keep.map(Height))?;
        }
        Ok(())
    }

    #[doc(hidden)]
    pub fn block_filter_table(&self) -> Option<&BlockFilterTable> {
        if self.block_filter_enabled() {
            self.block_filters.get()
        } else {
            None
        }
    }

    /// Heights the tip leads the committed filter watermark (0 when off).
    pub fn block_filter_lag_heights(&self) -> u32 {
        let Some(table) = self.block_filter_table() else {
            return 0;
        };
        match self.tip_height() {
            None => 0,
            Some(tip) => tip
                .0
                .saturating_add(1)
                .saturating_sub(table.next_height().0),
        }
    }

    /// Last committed filter height.
    pub fn basic_filter_hwm(&self) -> Result<Option<u32>, QueryError> {
        Ok(self
            .block_filter_table()
            .and_then(|t| t.next_height().0.checked_sub(1)))
    }

    /// Filter bytes and header for `hash`.
    ///
    /// A sealed best-chain row is that row. Otherwise the parent filter
    /// header is resolved first: a sealed best-chain parent, the zero
    /// genesis header, or a stale branch walked back to that sealed fork
    /// point. An unsealed best-chain gap is refused before the body is
    /// rebuilt.
    ///
    /// `None` is a block that never connected: an unknown hash, or a header
    /// off the best chain with no stored body. That is answered before the
    /// parent walk, so the parent's state does not answer for it.
    pub fn basic_filter_for_hash(
        &self,
        hash: &[u8; 32],
    ) -> Result<Option<(Vec<u8>, FilterHeader)>, QueryError> {
        self.basic_filter_for_hash_within(hash, MAX_STALE_FILTER_WALK)
    }

    /// [`Self::basic_filter_for_hash`] rebuilding at most `max_stale` stale
    /// blocks, the requested one included.
    #[doc(hidden)]
    pub fn basic_filter_for_hash_within(
        &self,
        hash: &[u8; 32],
        max_stale: usize,
    ) -> Result<Option<(Vec<u8>, FilterHeader)>, QueryError> {
        let mut height = self.height_of_hash(hash)?;
        if let Some(h) = height {
            match self.basic_filter_at(h.0) {
                Ok(Some(row)) => {
                    // A reload inside the repair can observe a block that
                    // replaced `h`. That row is not this hash, and the hash
                    // may have left the best chain during the read.
                    let now = self.height_of_hash(hash)?;
                    if now == Some(h) && self.stored_row_is_hash(h.0, hash, &row.0)? {
                        return Ok(Some(row));
                    }
                    height = now;
                }
                Ok(None) => {}
                Err(e) => return Err(e),
            }
        }
        let Some((header_fk, _)) = self.get_header_by_hash(hash)? else {
            return Ok(None);
        };
        if height.is_none() && !self.store.header_txs.has_body(header_fk)? {
            return Ok(None);
        }
        // A block off the best chain is itself one of the stale blocks
        // rebuilt. A best-chain block's parent is never stale.
        let ancestors = if height.is_some() {
            max_stale
        } else {
            max_stale
                .checked_sub(1)
                .ok_or(StoreError::Rejected(FILTER_STALE_TOO_DEEP))?
        };
        let parent_header =
            self.parent_basic_filter_header_within(&self.prev_header_hash(hash)?, ancestors)?;
        let block = self
            .block_body_for_filter(hash)?
            .ok_or(StoreError::Corrupt("invariant: blockfilter body missing"))?;
        let spent = self.spent_scripts(&block)?;
        let filter = self.basic_filter_content(&block, &spent)?;
        Ok(Some((
            filter.content.clone(),
            filter.filter_header(&parent_header),
        )))
    }

    /// The idx row at `height` still names `hash`, and `body` is that row.
    fn stored_row_is_hash(
        &self,
        height: u32,
        hash: &[u8; 32],
        body: &[u8],
    ) -> Result<bool, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(false);
        };
        let Some((fk, _)) = self.get_header_by_hash(hash)? else {
            return Ok(false);
        };
        let Some(slot) = table.slot(Height(height))? else {
            return Ok(false);
        };
        Ok(slot.header_fk == fk && check_filter_hash(body, &slot).is_ok())
    }

    #[cfg(debug_assertions)]
    #[doc(hidden)]
    pub fn basic_filter_from_wire_block(
        &self,
        block: &Block,
    ) -> Result<(Vec<u8>, FilterHeader), QueryError> {
        let spent = self.spent_scripts(block)?;
        let filter = self.basic_filter_content(block, &spent)?;
        let prev = self.parent_basic_filter_header(&block.header.prev_blockhash.to_byte_array())?;
        let header = filter.filter_header(&prev);
        Ok((filter.content.clone(), header))
    }

    #[doc(hidden)]
    pub fn basic_filter_content(
        &self,
        block: &Block,
        spent: &[Vec<u8>],
    ) -> Result<BlockFilter, QueryError> {
        let hash = block.block_hash().to_byte_array();
        let outputs: Vec<&[u8]> = block
            .txdata
            .iter()
            .flat_map(|tx| tx.output.iter().map(|o| o.script_pubkey.as_bytes()))
            .collect();
        let spent_refs: Vec<&[u8]> = spent.iter().map(|s| s.as_slice()).collect();
        basic_filter_from_scripts(&hash, outputs, spent_refs)
    }

    /// Header of `prev_hash`. Zeros are the genesis prev-header. A sealed
    /// best-chain block is that row. A stale block is rebuilt back to the
    /// best-chain fork point, and only when that fork point is sealed and at
    /// most [`MAX_STALE_FILTER_WALK`] stale blocks away.
    #[doc(hidden)]
    pub fn parent_basic_filter_header(
        &self,
        prev_hash: &[u8; 32],
    ) -> Result<FilterHeader, QueryError> {
        self.parent_basic_filter_header_within(prev_hash, MAX_STALE_FILTER_WALK)
    }

    /// [`Self::parent_basic_filter_header`] with at most `max_stale` stale
    /// blocks rebuilt. One more is [`FILTER_STALE_TOO_DEEP`], found from
    /// headers alone before any body is read.
    #[doc(hidden)]
    pub fn parent_basic_filter_header_within(
        &self,
        prev_hash: &[u8; 32],
        max_stale: usize,
    ) -> Result<FilterHeader, QueryError> {
        if *prev_hash == [0u8; 32] {
            return Ok(FilterHeader::from_byte_array([0u8; 32]));
        }
        let mut pending = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut cursor = *prev_hash;
        let anchor = loop {
            if cursor == [0u8; 32] {
                break FilterHeader::from_byte_array([0u8; 32]);
            }
            if let Some(h) = self.height_of_hash(&cursor)? {
                if let Some((_, header)) = self.basic_filter_at(h.0)? {
                    break header;
                }
                return Err(StoreError::Rejected(FILTER_STILL_INDEXING));
            }
            if !seen.insert(cursor) {
                return Err(StoreError::Corrupt("invariant: blockfilter header cycle"));
            }
            if pending.len() >= max_stale {
                return Err(StoreError::Rejected(FILTER_STALE_TOO_DEEP));
            }
            pending.push(cursor);
            cursor = self.prev_header_hash(&cursor)?;
        };
        let mut header = anchor;
        for hash in pending.iter().rev() {
            let Some(block) = self.block_body_for_filter(hash)? else {
                return Err(StoreError::Corrupt(
                    "invariant: blockfilter parent body missing",
                ));
            };
            let spent = self.spent_scripts(&block)?;
            let filter = self.basic_filter_content(&block, &spent)?;
            header = filter.filter_header(&header);
        }
        Ok(header)
    }

    #[doc(hidden)]
    pub fn header_prev_hash(&self, hash: &[u8; 32]) -> Result<Option<[u8; 32]>, QueryError> {
        let Some((_, rec)) = self.get_header_by_hash(hash)? else {
            return Ok(None);
        };
        if rec.prev_fk.is_null() {
            return Ok(Some([0u8; 32]));
        }
        Ok(Some(self.get_header(rec.prev_fk)?.hash))
    }

    #[doc(hidden)]
    pub fn prev_header_hash(&self, hash: &[u8; 32]) -> Result<[u8; 32], QueryError> {
        self.header_prev_hash(hash)?.ok_or(StoreError::Corrupt(
            "invariant: blockfilter parent header missing",
        ))
    }

    #[doc(hidden)]
    pub fn block_body_for_filter(&self, hash: &[u8; 32]) -> Result<Option<Block>, QueryError> {
        if let Some(h) = self.height_of_hash(hash)? {
            return Ok(Some(self.reconstruct_block_at_height(h)?));
        }
        self.reconstruct_archived_block(hash)
    }

    #[doc(hidden)]
    pub fn spent_scripts(&self, block: &Block) -> Result<Vec<Vec<u8>>, QueryError> {
        let mut spent = Vec::new();
        for tx in &block.txdata {
            for inp in &tx.input {
                if inp.previous_output == OutPoint::null() {
                    continue;
                }
                let txid = inp.previous_output.txid.to_byte_array();
                let Some((_, rec)) = self.get_tx_by_txid(&txid)? else {
                    return Err(StoreError::Corrupt(
                        "invariant: blockfilter prevout missing",
                    ));
                };
                let out = self.tx_output(&rec, inp.previous_output.vout)?;
                spent.push(out.script);
            }
        }
        Ok(spent)
    }

    fn repaired_filter_body(
        &self,
        height: u32,
        mut body: Vec<u8>,
        slot: &BlockFilterSlot,
    ) -> Result<(Vec<u8>, BlockFilterSlot), QueryError> {
        let mut slot = *slot;
        for _ in 0..2 {
            if check_filter_hash(&body, &slot).is_ok() {
                return Ok((body, slot));
            }
            let Some(table) = self.block_filter_table() else {
                return Err(StoreError::Corrupt(
                    "invariant: blockfilter body does not match its index hash",
                ));
            };
            let block = self.reconstruct_block_at_height(Height(height))?;
            let spent = self.spent_scripts(&block)?;
            let filter = self.basic_filter_content(&block, &spent)?;
            if FilterHash::hash(&filter.content).to_byte_array() != slot.filter_hash {
                return Err(StoreError::Corrupt(
                    "invariant: blockfilter body does not match its index hash",
                ));
            }
            if table.repair_body(Height(height), &slot, &filter.content)? {
                return Ok((filter.content, slot));
            }
            match table.filter(Height(height))? {
                Some((next, next_slot)) => {
                    body = next;
                    slot = next_slot;
                }
                None => return Err(StoreError::NotFound),
            }
        }
        if check_filter_hash(&body, &slot).is_ok() {
            return Ok((body, slot));
        }
        Err(StoreError::Corrupt(
            "invariant: blockfilter repair slot moved",
        ))
    }

    /// Filter bytes and header at `height`. `None` past the watermark.
    pub fn basic_filter_at(
        &self,
        height: u32,
    ) -> Result<Option<(Vec<u8>, FilterHeader)>, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(None);
        };
        let row = table
            .filter(Height(height))?
            .map(|(body, slot)| {
                let (body, slot) = self.repaired_filter_body(height, body, &slot)?;
                Ok::<_, QueryError>((body, FilterHeader::from_byte_array(slot.filter_header)))
            })
            .transpose()?;
        #[cfg(debug_assertions)]
        let row = lie_filter_row(row);
        Ok(row)
    }

    /// Filter bytes for `start..=end` in one idx and one body read per
    /// segment. `None` when the index is off or `end` is past the watermark.
    pub fn basic_filters(&self, start: u32, end: u32) -> Result<Option<Vec<Vec<u8>>>, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(None);
        };
        table
            .filters(Height(start), Height(end))?
            .map(|v| {
                v.into_iter()
                    .enumerate()
                    .map(|(i, (body, slot))| {
                        self.repaired_filter_body(start.saturating_add(i as u32), body, &slot)
                            .map(|(body, _slot)| body)
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()
    }

    /// Filter headers at heights 1,000, 2,000, … through `stop`.
    /// `None` when the index is off or `stop` is past the watermark.
    pub fn basic_filter_checkpoint_headers(
        &self,
        stop: u32,
    ) -> Result<Option<Vec<FilterHeader>>, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(None);
        };
        Ok(table.checkpoint_headers(Height(stop))?.map(|rows| {
            rows.into_iter()
                .map(FilterHeader::from_byte_array)
                .collect()
        }))
    }

    /// Filter hash and header for `start..=end` without filter bytes.
    /// `None` when the index is off or `end` is past the watermark.
    pub fn basic_filter_hashes_and_headers(
        &self,
        start: u32,
        end: u32,
    ) -> Result<Option<Vec<(FilterHash, FilterHeader)>>, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(None);
        };
        Ok(table.slots(Height(start), Height(end))?.map(|v| {
            v.into_iter()
                .map(|s| {
                    (
                        FilterHash::from_byte_array(s.filter_hash),
                        FilterHeader::from_byte_array(s.filter_header),
                    )
                })
                .collect()
        }))
    }

    /// Heights index write-behind may seal now: released through tip.
    pub fn index_target(&self) -> Option<u32> {
        let tip = self.tip_height()?.0;
        Some(self.index_released_through_height()?.min(tip))
    }

    #[doc(hidden)]
    pub fn commit_basic_filters(
        &self,
        table: &BlockFilterTable,
        start: u32,
        built: &[(BlockFilter, Fk)],
    ) -> Result<u32, QueryError> {
        let _appender = self.bf_wb.lock_appender();
        self.write_basic_filters(table, start, built, |recs| table.put(recs))
    }

    /// [`Self::commit_basic_filters`] without taking the appender lock.
    ///
    /// `commit` publishes the records. A return of 0 means the watermark or
    /// `confirmed[h]` moved and `commit` was not called.
    pub(crate) fn write_basic_filters(
        &self,
        table: &BlockFilterTable,
        start: u32,
        built: &[(BlockFilter, Fk)],
        commit: impl FnOnce(&[BlockFilterRecord<'_>]) -> Result<(), StoreError>,
    ) -> Result<u32, QueryError> {
        if table.next_height().0 != start {
            return Ok(0);
        }
        for (h, (_, header_fk)) in (start..).zip(built) {
            if self.store.confirmed.get(Height(h))? != Some(*header_fk) {
                return Ok(0);
            }
        }
        let mut prev = match start.checked_sub(1) {
            None => FilterHeader::from_byte_array([0u8; 32]),
            Some(h) => FilterHeader::from_byte_array(
                table
                    .slot(Height(h))?
                    .ok_or(StoreError::Corrupt(
                        "invariant: blockfilter slot below next",
                    ))?
                    .filter_header,
            ),
        };
        let mut recs = Vec::with_capacity(built.len());
        for (h, (filter, header_fk)) in (start..).zip(built) {
            let header = filter.filter_header(&prev);
            recs.push(BlockFilterRecord {
                height: Height(h),
                slot: BlockFilterSlot {
                    header_fk: *header_fk,
                    filter_hash: FilterHash::hash(&filter.content).to_byte_array(),
                    filter_header: header.to_byte_array(),
                },
                filter: &filter.content,
            });
            prev = header;
        }
        commit(&recs)?;
        Ok(built.len() as u32)
    }

    /// Confirm may append filter and tweak rows for batches it connects.
    pub fn index_live(&self) -> bool {
        self.index_live.load(Ordering::Acquire)
    }

    /// The write side of [`Self::index_live`]. Startup sets it; confirm does not.
    pub fn set_index_live(&self, on: bool) {
        self.index_live.store(on, Ordering::Release);
    }

    /// Pruned tweak append is on. Independent of [`Self::index_live`].
    pub fn tweak_follow(&self) -> bool {
        self.tweak_follow.load(Ordering::Acquire)
    }

    /// The write side of [`Self::tweak_follow`].
    pub fn set_tweak_follow(&self, on: bool) {
        self.tweak_follow.store(on, Ordering::Release);
    }

    /// Next filter height to seal (`None` when the filter index is off).
    pub fn filter_index_next(&self) -> Option<u32> {
        self.block_filter_table().map(|t| t.next_height().0)
    }

    /// Commit filters built for `start..` (a window). Returns heights
    /// committed; 0 when the watermark or a `confirmed[h]` moved (a reorg).
    pub fn commit_window_filters(
        &self,
        start: u32,
        built: &[(BlockFilter, Fk)],
    ) -> Result<u32, QueryError> {
        let Some(table) = self.block_filter_table() else {
            return Ok(0);
        };
        self.commit_basic_filters(table, start, built)
    }

    /// Wait for an index release (or `d`).
    pub fn wait_index_release(&self, d: std::time::Duration) {
        self.bf_wb.wait(d);
    }

    pub fn truncate_basic_filters_to_tip(&self) -> Result<(), QueryError> {
        match self.block_filters.get() {
            Some(t) => Ok(t.truncate_through(self.tip_height())?),
            None => Ok(()),
        }
    }
}
