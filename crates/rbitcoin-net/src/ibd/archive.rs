//! Archive prep + writer pipeline for IBD.

use super::coalesce::{coalesce_wait, max_batch_for_lag, min_batch_for_queue};
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash};
use rbitcoin_consensus::prepare_block_for_archive_ibd;
use rbitcoin_log::debug;
use rbitcoin_primitives::Fk;
use rbitcoin_query::TxApply;
use rbitcoin_store::HeaderRecord;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Shared archive-pipeline timers for IBD status (atomics; reset on sample).
#[derive(Default)]
pub(crate) struct ArchivePipelineStats {
    pub(crate) prep_ns: AtomicU64,
    pub(crate) prep_blocks: AtomicU64,
    pub(crate) write_ns: AtomicU64,
    pub(crate) write_batches: AtomicU64,
    pub(crate) write_blocks: AtomicU64,
    pub(crate) write_batch_blocks: AtomicU64,
    pub(crate) write_idle_ns: AtomicU64,
    pub(crate) write_coalesce_ns: AtomicU64,
}

impl ArchivePipelineStats {
    pub(crate) fn sample_and_reset(&self) -> ArchivePipelineSample {
        let prep_ns = self.prep_ns.swap(0, Ordering::Relaxed);
        let prep_blocks = self.prep_blocks.swap(0, Ordering::Relaxed);
        let write_ns = self.write_ns.swap(0, Ordering::Relaxed);
        let write_batches = self.write_batches.swap(0, Ordering::Relaxed);
        let write_blocks = self.write_blocks.swap(0, Ordering::Relaxed);
        let write_batch_blocks = self.write_batch_blocks.swap(0, Ordering::Relaxed);
        let write_idle_ns = self.write_idle_ns.swap(0, Ordering::Relaxed);
        let write_coalesce_ns = self.write_coalesce_ns.swap(0, Ordering::Relaxed);
        ArchivePipelineSample {
            prep_ns,
            prep_blocks,
            write_ns,
            write_batches,
            write_blocks,
            write_batch_blocks,
            write_idle_ns,
            write_coalesce_ns,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ArchivePipelineSample {
    pub(crate) prep_ns: u64,
    pub(crate) prep_blocks: u64,
    pub(crate) write_ns: u64,
    pub(crate) write_batches: u64,
    pub(crate) write_blocks: u64,
    pub(crate) write_batch_blocks: u64,
    pub(crate) write_idle_ns: u64,
    pub(crate) write_coalesce_ns: u64,
}

impl ArchivePipelineSample {
    pub(crate) fn prep_us_per_block(&self) -> u64 {
        if self.prep_blocks == 0 {
            0
        } else {
            (self.prep_ns / self.prep_blocks) / 1000
        }
    }
    pub(crate) fn write_us_per_block(&self) -> u64 {
        if self.write_blocks == 0 {
            0
        } else {
            (self.write_ns / self.write_blocks) / 1000
        }
    }
    pub(crate) fn avg_batch(&self) -> u64 {
        if self.write_batches == 0 {
            0
        } else {
            self.write_batch_blocks / self.write_batches
        }
    }
    pub(crate) fn write_busy_ms(&self) -> u64 {
        self.write_ns / 1_000_000
    }
    pub(crate) fn write_idle_ms(&self) -> u64 {
        self.write_idle_ns / 1_000_000
    }
    pub(crate) fn write_coalesce_ms(&self) -> u64 {
        self.write_coalesce_ns / 1_000_000
    }
    pub(crate) fn prep_ms(&self) -> u64 {
        self.prep_ns / 1_000_000
    }
}


/// Default RAM budget for decoded blocks waiting in the archive pipeline (~512 MiB).
/// Override with env `RBITCOIN_ARCHIVE_QUEUE_MB`.
///
/// Sized so network stays busy and ContigPark can form mega-batches without a
/// multi‑GiB junkyard. Wire-size undercounts true RSS of decoded `Block` + prep
/// (×1.5 charge); still stacked with prewarm mlock + page cache.
pub const DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Enter “pressure” (far_scale = 0) when fill ≥ this fraction of budget.
pub const ARCHIVE_PRESSURE_ENTER: f64 = 0.90;
/// Leave pressure only after fill ≤ this (hysteresis vs enter).
pub const ARCHIVE_PRESSURE_EXIT: f64 = 0.70;

/// Shared counter of blocks (and approx wire bytes) in the archive pipeline.
///
/// Charged when a decoded body is handed to the job channel; released when the
/// writer (or prep error path) returns [`ArchiveResult`]. Used for getdata
/// backpressure so RAM waiting on archive stays near the configured budget.
///
/// **Hard cap:** [`Self::try_charge`] refuses admission once charged bytes would
/// exceed the budget (empty queue still admits one). Soft densify scaling via
/// [`Self::far_admission_scale`] (proportional headroom + 90%/70% pressure
/// hysteresis) only reduces far getdata — it does not bound the job channel.
pub(crate) struct ArchiveQueueBudget {
    count: AtomicUsize,
    bytes: AtomicUsize,
    budget: usize,
    /// Latched high-fill mode: once ≥ [`ARCHIVE_PRESSURE_ENTER`], stays until
    /// ≤ [`ARCHIVE_PRESSURE_EXIT`].
    pressure: AtomicBool,
}

impl ArchiveQueueBudget {
    pub fn new(budget: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            // At least 16 MiB so tiny overrides still leave room for a few blocks.
            budget: budget.max(16 * 1024 * 1024),
            pressure: AtomicBool::new(false),
        }
    }

    pub fn from_env() -> Arc<Self> {
        let budget = std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .unwrap_or(DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES);
        Arc::new(Self::new(budget))
    }

    pub fn budget_bytes(&self) -> usize {
        self.budget
    }

    pub fn count(&self) -> usize {
        self.count.load(Ordering::Relaxed)
    }

    pub fn bytes(&self) -> usize {
        self.bytes.load(Ordering::Relaxed)
    }

    /// Charged bytes / budget (may be > 1 when oversubscribed).
    pub fn fill_ratio(&self) -> f64 {
        let b = self.budget.max(1) as f64;
        self.bytes() as f64 / b
    }

    /// Whether pressure hysteresis is latched (far densify off).
    #[cfg(test)]
    pub fn in_pressure(&self) -> bool {
        self.pressure.load(Ordering::Relaxed)
    }

    /// Update pressure latch from current fill; return far admission scale in **0..=1**.
    ///
    /// - **Pressure (A):** enter at fill ≥ 0.90, exit only at fill ≤ 0.70 → scale 0.
    /// - **Proportional (B):** outside pressure, `scale = (1 - fill).clamp(0, 1)`
    ///   so half-full budget ≈ half far work (smooth BW, no cliff at budget).
    ///
    /// ContigPark gap + tip-near are **not** gated by this (assign always covers them).
    pub fn far_admission_scale(&self) -> f64 {
        let fill = self.fill_ratio();
        let was = self.pressure.load(Ordering::Relaxed);
        let (scale, pressure) = Self::far_scale_from(fill, was);
        self.pressure.store(pressure, Ordering::Relaxed);
        scale
    }

    /// Pure helper: scale from fill + pressure latch (shared by production + tests).
    pub(crate) fn far_scale_from(fill: f64, mut pressure: bool) -> (f64, bool) {
        if fill >= ARCHIVE_PRESSURE_ENTER {
            pressure = true;
        } else if fill <= ARCHIVE_PRESSURE_EXIT {
            pressure = false;
        }
        let scale = if pressure {
            0.0
        } else {
            (1.0 - fill).clamp(0.0, 1.0)
        };
        (scale, pressure)
    }

    /// Unconditional charge (tests / forced overshoot). Hot path uses [`try_charge`].
    #[cfg(test)]
    pub fn charge(&self, wire_bytes: usize) {
        let charged = Self::charged_bytes(wire_bytes);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(charged, Ordering::Relaxed);
    }

    /// Same overhead as [`charge`] — callers must release the charged amount.
    pub fn charged_bytes(wire_bytes: usize) -> usize {
        wire_bytes.saturating_mul(3).saturating_add(4096) / 2
    }

    /// Hard admission: charge only if current fill + this block stays ≤ budget.
    ///
    /// Always admits when the queue is empty (so a single huge block can still
    /// enter). Returns `false` without mutating counters when full — caller must
    /// **not** put the body in the pipeline (mark_missing / re-getdata later).
    ///
    /// Soft [`far_admission_scale`] alone is not enough: ContigPark densify and
    /// pending redelivery kept charging into an unbounded job channel until
    /// mainnet held ~50k decoded bodies (~60 GiB) against a 512 MiB budget.
    pub fn try_charge(&self, wire_bytes: usize) -> bool {
        let charged = Self::charged_bytes(wire_bytes);
        loop {
            let cur = self.bytes.load(Ordering::Relaxed);
            // Empty queue: always admit one (progress / tip-hole even if one
            // block's charged size exceeds budget).
            if cur > 0 && cur.saturating_add(charged) > self.budget {
                return false;
            }
            match self.bytes.compare_exchange_weak(
                cur,
                cur.saturating_add(charged),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.count.fetch_add(1, Ordering::Relaxed);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    /// Release after archive Ok/Err (or failed send into a closed pipeline).
    ///
    /// Pass the original **wire** size; overhead is re-derived via [`charged_bytes`].
    pub fn release(&self, wire_bytes: usize) {
        let charged = Self::charged_bytes(wire_bytes);
        let _ = self.count.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(1))
        });
        let _ = self.bytes.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
            Some(n.saturating_sub(charged))
        });
    }
}


/// Wire block queued for archive prep.
pub(crate) struct ArchiveJob {
    pub block: Block,
    /// Header row fk from ensure_header (writer skips hash-head re-lookup).
    pub header_fk: Fk,
    /// Tip-near body: prep/writer jump the FIFO so confirm is not stuck behind
    /// far archive-lead blocks.
    pub priority: bool,
    /// Approx serialized size charged against [`ArchiveQueueBudget`].
    pub wire_bytes: usize,
    /// Chain height when known (`u32::MAX` if unknown). Required for contiguous
    /// mega-batch assembly (schema v10 create_fk needs parents already written).
    pub height: u32,
}

struct PreparedArchive {
    hash: BlockHash,
    header: HeaderRecord,
    txs: Vec<TxApply>,
    header_fk: Fk,
    wire_bytes: usize,
    height: u32,
}

/// Parks prepared bodies until a **height-contiguous** mega-batch can start at
/// `next_h`. Out-of-order peer delivery stays in RAM here; only contiguous runs
/// are passed to Class A mega-write (fast create_fk resolve via batch_map/sticky).
struct ContigPark {
    /// Next height the writer may commit (contiguous HWM + 1).
    next_h: u32,
    /// `height → prepared` for heights ≥ `next_h`.
    parked: BTreeMap<u32, PreparedArchive>,
}

/// Result of trying to park a prepared body.
enum ParkInsert {
    /// Stored; wait for contiguous prefix.
    Parked,
    /// Height already past HWM or unknown — caller should single-archive / fail.
    Late(PreparedArchive),
    /// Same height already parked (multi-peer redelivery). Caller must release
    /// the **second** charge via [`ArchiveResult::Dropped`] — **not** Ok
    /// (body is not written yet; Ok would false-mark Class A for confirm).
    Duplicate(PreparedArchive),
    /// Too far ahead of `next_h` — refuse park so RAM stays near ContigPark head.
    /// Caller releases charge and requeues getdata for later.
    BeyondHorizon(PreparedArchive),
}

impl ContigPark {
    fn new(next_h: u32) -> Self {
        Self {
            next_h,
            parked: BTreeMap::new(),
        }
    }

    fn next_h(&self) -> u32 {
        self.next_h
    }

    fn parked_len(&self) -> usize {
        self.parked.len()
    }

    /// `horizon` = max offset past `next_h` we will hold (e.g. [`super::CONTIG_DENSIFY_AHEAD`]).
    fn insert(&mut self, p: PreparedArchive, horizon: u32) -> ParkInsert {
        if p.height == u32::MAX {
            return ParkInsert::Late(p);
        }
        if p.height < self.next_h {
            return ParkInsert::Late(p);
        }
        if p.height > self.next_h.saturating_add(horizon) {
            return ParkInsert::BeyondHorizon(p);
        }
        use std::collections::btree_map::Entry;
        match self.parked.entry(p.height) {
            Entry::Vacant(e) => {
                e.insert(p);
                ParkInsert::Parked
            }
            Entry::Occupied(_) => ParkInsert::Duplicate(p),
        }
    }

    /// How many contiguous heights are ready starting at `next_h`.
    fn ready_prefix_len(&self) -> usize {
        let mut n = 0u32;
        loop {
            if self.parked.contains_key(&self.next_h.saturating_add(n)) {
                n += 1;
            } else {
                break;
            }
        }
        n as usize
    }

    /// Advance `next_h` without writing (caller verified Class A already holds
    /// that height — e.g. Late single-archive or resume HWM).
    fn force_advance(&mut self, n: u32) {
        if n == 0 {
            return;
        }
        self.next_h = self.next_h.saturating_add(n);
        // Drop any parked entries that are now past (shouldn't exist).
        while let Some((&h, _)) = self.parked.first_key_value() {
            if h < self.next_h {
                self.parked.remove(&h);
            } else {
                break;
            }
        }
    }

    /// Pop a contiguous run `[next_h, next_h+len)` of at most `max` blocks.
    /// Advances `next_h` by the number taken (caller must [`rewind`] on write failure).
    fn take_contiguous(&mut self, max: usize) -> Vec<PreparedArchive> {
        if max == 0 {
            return Vec::new();
        }
        let mut out = Vec::with_capacity(max.min(64));
        while out.len() < max {
            let h = self.next_h.saturating_add(out.len() as u32);
            match self.parked.remove(&h) {
                Some(p) => out.push(p),
                None => break,
            }
        }
        if !out.is_empty() {
            self.next_h = self.next_h.saturating_add(out.len() as u32);
        }
        out
    }

    /// Undo a failed mega-write that already advanced `next_h`.
    fn rewind(&mut self, n: u32) {
        self.next_h = self.next_h.saturating_sub(n);
    }

    /// Drain all parked bodies (shutdown). Caller must release each charge.
    fn drain_all(&mut self) -> Vec<PreparedArchive> {
        std::mem::take(&mut self.parked).into_values().collect()
    }
}

#[cfg(test)]
mod contig_park_tests {
    use super::ContigPark;
    use bitcoin::hashes::Hash;
    use bitcoin::BlockHash;
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::HeaderRecord;

    fn prep(h: u32) -> super::PreparedArchive {
        super::PreparedArchive {
            hash: BlockHash::from_byte_array([h as u8; 32]),
            header: HeaderRecord {
                prev_fk: Fk::NULL,
                version: 1,
                timestamp: 0,
                bits: 0,
                nonce: 0,
                merkle_root: [0u8; 32],
                hash: [h as u8; 32],
            },
            txs: vec![],
            header_fk: Fk(h as u64 + 1),
            wire_bytes: 100,
            height: h,
        }
    }

    #[test]
    fn parks_ahead_until_gap_filled() {
        let mut p = ContigPark::new(10);
        const H: u32 = 2048;
        assert!(matches!(p.insert(prep(12), H), super::ParkInsert::Parked));
        assert!(matches!(p.insert(prep(11), H), super::ParkInsert::Parked));
        assert!(p.take_contiguous(8).is_empty(), "gap at 10");
        assert_eq!(p.parked_len(), 2);
        assert!(matches!(p.insert(prep(10), H), super::ParkInsert::Parked));
        let run = p.take_contiguous(8);
        assert_eq!(run.len(), 3);
        assert_eq!(run[0].height, 10);
        assert_eq!(run[2].height, 12);
        assert_eq!(p.next_h(), 13);
        assert_eq!(p.parked_len(), 0);
    }

    #[test]
    fn late_height_returned_for_idempotent_path() {
        let mut p = ContigPark::new(5);
        match p.insert(prep(3), 2048) {
            super::ParkInsert::Late(late) => assert_eq!(late.height, 3),
            _ => panic!("expected Late"),
        }
        assert_eq!(p.parked_len(), 0);
    }

    #[test]
    fn beyond_horizon_refused() {
        let mut p = ContigPark::new(10);
        match p.insert(prep(10 + 2049), 2048) {
            super::ParkInsert::BeyondHorizon(f) => assert_eq!(f.height, 10 + 2049),
            _ => panic!("expected BeyondHorizon"),
        }
        assert_eq!(p.parked_len(), 0);
        assert!(matches!(p.insert(prep(10 + 2048), 2048), super::ParkInsert::Parked));
    }

    #[test]
    fn duplicate_height_returns_dup_for_budget_release_only() {
        // Multi-peer redelivery: caller must ArchiveResult::Dropped (not Ok).
        let mut p = ContigPark::new(1);
        assert!(matches!(p.insert(prep(1), 2048), super::ParkInsert::Parked));
        match p.insert(prep(1), 2048) {
            super::ParkInsert::Duplicate(d) => {
                assert_eq!(d.height, 1);
                assert_eq!(d.hash, prep(1).hash);
            }
            _ => panic!("expected Duplicate"),
        }
        // Original remains parked until contiguous write.
        assert_eq!(p.parked_len(), 1);
    }

    #[test]
    fn caps_run_at_max() {
        let mut p = ContigPark::new(0);
        for h in 0..10 {
            assert!(matches!(p.insert(prep(h), 2048), super::ParkInsert::Parked));
        }
        let run = p.take_contiguous(4);
        assert_eq!(run.len(), 4);
        assert_eq!(p.next_h(), 4);
        assert_eq!(p.parked_len(), 6);
    }

    #[test]
    fn force_advance_unblocks_parked_prefix() {
        let mut p = ContigPark::new(10);
        assert!(matches!(p.insert(prep(12), 2048), super::ParkInsert::Parked));
        assert!(matches!(p.insert(prep(11), 2048), super::ParkInsert::Parked));
        assert!(p.take_contiguous(8).is_empty());
        // Simulate 10 already Class A — skip without a parked body.
        p.force_advance(1);
        assert_eq!(p.next_h(), 11);
        let run = p.take_contiguous(8);
        assert_eq!(run.len(), 2);
        assert_eq!(run[0].height, 11);
        assert_eq!(run[1].height, 12);
        assert_eq!(p.next_h(), 13);
    }
}

pub(crate) enum ArchiveResult {
    Ok {
        hash: BlockHash,
        wire_bytes: usize,
    },
    Err {
        hash: BlockHash,
        err: String,
        wire_bytes: usize,
    },
    /// Release pipeline budget only — body was never Class-A written.
    ///
    /// Used for multi-peer redelivery while the first copy is still in
    /// [`ContigPark`], and for beyond-horizon refuse. Must **not** call
    /// `mark_archived`. When `requeue`, mark_missing so densify can re-getdata
    /// once `write_next` advances into range.
    Dropped {
        hash: BlockHash,
        wire_bytes: usize,
        requeue: bool,
    },
}

/// Dual-lane prep + writer: tip-near priority jumps far FIFO into a **contiguous
/// height park**. Schema v10 create_fk resolve needs parents already written (or
/// in the same mega-batch); the writer only mega-archives height-contiguous runs.
///
/// Out-of-order bodies sit in [`ContigPark`] until `next_h` arrives — no per-block
/// fallback thrash.
pub(crate) fn spawn_archive_pipeline(
    hub: Arc<ChainHub>,
    mut job_rx: mpsc::UnboundedReceiver<ArchiveJob>,
    result_tx: mpsc::UnboundedSender<ArchiveResult>,
    stats: Arc<ArchivePipelineStats>,
    archive_queued: Arc<ArchiveQueueBudget>,
    confirm_lag: Arc<AtomicU32>,
    // Next height the writer may commit (contiguous archived HWM + 1).
    write_next_height: Arc<AtomicU32>,
    // Cooperative stop (SIGINT): exit after current write batch; drop queue.
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        const WRITE_Q: usize = 4096;
        const PRI_Q: usize = 512;
        debug!("ibd: archive pipeline prep=1 (OS thread) writer=1 (OS thread) dual-lane contig-park");

        let (pri_write_tx, pri_write_rx) =
            std::sync::mpsc::sync_channel::<PreparedArchive>(PRI_Q);
        let (far_write_tx, far_write_rx) =
            std::sync::mpsc::sync_channel::<PreparedArchive>(WRITE_Q);
        let write_q_depth = Arc::new(AtomicUsize::new(0));

        let write_hub = hub.clone();
        let write_result = result_tx.clone();
        let write_stats = Arc::clone(&stats);
        let write_arch_q = Arc::clone(&archive_queued);
        let write_depth = Arc::clone(&write_q_depth);
        let write_lag = Arc::clone(&confirm_lag);
        let write_next = Arc::clone(&write_next_height);
        let write_stop = Arc::clone(&stop);

        let writer = std::thread::Builder::new()
            .name("ibd-archive-writer".into())
            .spawn(move || {
                rbitcoin_store::try_set_io_idle();
                const FLUSH_EVERY_BLOCKS: u64 = 8192;
                let mut blocks_since_flush = 0u64;
                let mut pri_open = true;
                let mut far_open = true;
                let mut park = ContigPark::new(write_next.load(Ordering::Relaxed));
                // Refuse park beyond ContigPark head + densify band (same as assign).
                let park_horizon = super::CONTIG_DENSIFY_AHEAD;

                /// Park one prepared body; late/dup must always emit ArchiveResult
                /// so archive_queued charge is released (else IBD never path_drains).
                fn handle_insert(
                    park: &mut ContigPark,
                    p: PreparedArchive,
                    write_hub: &ChainHub,
                    write_result: &mpsc::UnboundedSender<ArchiveResult>,
                    park_horizon: u32,
                ) -> bool {
                    match park.insert(p, park_horizon) {
                        ParkInsert::Parked => true,
                        ParkInsert::Duplicate(dup) => {
                            // Multi-peer redelivery while first is still parked:
                            // free the **duplicate** charge only — do not claim Ok.
                            write_result
                                .send(ArchiveResult::Dropped {
                                    hash: dup.hash,
                                    wire_bytes: dup.wire_bytes,
                                    requeue: false,
                                })
                                .is_ok()
                        }
                        ParkInsert::BeyondHorizon(far) => {
                            // Too far ahead of ContigPark head — free RAM; re-getdata later.
                            write_result
                                .send(ArchiveResult::Dropped {
                                    hash: far.hash,
                                    wire_bytes: far.wire_bytes,
                                    requeue: true,
                                })
                                .is_ok()
                        }
                        ParkInsert::Late(late) => {
                            let hash = late.hash;
                            let wire_bytes = late.wire_bytes;
                            if late.height == u32::MAX {
                                return write_result
                                    .send(ArchiveResult::Err {
                                        hash,
                                        err: "archive: missing height for contiguous batch"
                                            .into(),
                                        wire_bytes,
                                    })
                                    .is_ok();
                            }
                            let mut one = [(late.header_fk, late.header, late.txs)];
                            let res = match write_hub.query.archive_prepared_with_fks(&mut one)
                            {
                                Ok(_) => ArchiveResult::Ok { hash, wire_bytes },
                                Err(e) => ArchiveResult::Err {
                                    hash,
                                    err: e.to_string(),
                                    wire_bytes,
                                },
                            };
                            write_result.send(res).is_ok()
                        }
                    }
                }

                loop {
                    if write_stop.load(Ordering::Relaxed) {
                        while let Ok(p) = pri_write_rx.try_recv() {
                            write_q_dec(&write_depth);
                            let _ = write_result.send(ArchiveResult::Err {
                                hash: p.hash,
                                err: "archive stopped".into(),
                                wire_bytes: p.wire_bytes,
                            });
                        }
                        while let Ok(p) = far_write_rx.try_recv() {
                            write_q_dec(&write_depth);
                            let _ = write_result.send(ArchiveResult::Err {
                                hash: p.hash,
                                err: "archive stopped".into(),
                                wire_bytes: p.wire_bytes,
                            });
                        }
                        for p in park.drain_all() {
                            let _ = write_result.send(ArchiveResult::Err {
                                hash: p.hash,
                                err: "archive stopped".into(),
                                wire_bytes: p.wire_bytes,
                            });
                        }
                        break;
                    }
                    if !pri_open && !far_open && park.parked_len() == 0 {
                        break;
                    }

                    let idle_t0 = Instant::now();
                    // Pull at least one prepared body (or timeout to re-check stop / park).
                    let first = match pri_write_rx.try_recv() {
                        Ok(p) => Some(p),
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            pri_open = false;
                            match far_write_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(p) => Some(p),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    None
                                }
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            match far_write_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(p) => Some(p),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    match pri_write_rx.try_recv() {
                                        Ok(p) => Some(p),
                                        Err(std::sync::mpsc::TryRecvError::Empty) => None,
                                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                            pri_open = false;
                                            None
                                        }
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    match pri_write_rx.try_recv() {
                                        Ok(p) => Some(p),
                                        Err(_) => None,
                                    }
                                }
                            }
                        }
                    };

                    if let Some(p) = first {
                        write_q_dec(&write_depth);
                        if !handle_insert(
                            &mut park,
                            p,
                            &write_hub,
                            &write_result,
                            park_horizon,
                        ) {
                            return;
                        }
                    }
                    write_stats
                        .write_idle_ns
                        .fetch_add(idle_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    // Drain everything currently available into the park.
                    let coal_t0 = Instant::now();
                    loop {
                        let mut got = false;
                        while let Ok(p) = pri_write_rx.try_recv() {
                            write_q_dec(&write_depth);
                            if !handle_insert(
                                &mut park,
                                p,
                                &write_hub,
                                &write_result,
                                park_horizon,
                            ) {
                                return;
                            }
                            got = true;
                        }
                        while let Ok(p) = far_write_rx.try_recv() {
                            write_q_dec(&write_depth);
                            if !handle_insert(
                                &mut park,
                                p,
                                &write_hub,
                                &write_result,
                                park_horizon,
                            ) {
                                return;
                            }
                            got = true;
                        }
                        if !got {
                            break;
                        }
                    }

                    let lag = write_lag.load(Ordering::Relaxed);
                    let arch_q_n = write_arch_q.count();
                    let max_mega = max_batch_for_lag(lag);
                    let min_batch = min_batch_for_queue(arch_q_n, lag);
                    let mut ready = park.ready_prefix_len();

                    // If the park is waiting on a height already Class A (Late
                    // path / prior write), advance so parked higher heights drain.
                    // Without this, a full budget of non-contiguous park + missing
                    // re-getdata can freeze write_next forever.
                    if ready == 0 && park.parked_len() > 0 {
                        let mut skipped = 0u32;
                        while skipped < 64 && park.ready_prefix_len() == 0 {
                            let nh = park.next_h();
                            // Prefer process-local height map via header hash on
                            // the prepared park (higher keys) is not next_h.
                            // Probe store: confirmed[] only covers tip; use
                            // header_at_height when confirmed, else stop.
                            let archived = match write_hub.query.header_at_height(
                                rbitcoin_primitives::Height(nh),
                            ) {
                                Ok(Some((_, rec))) => {
                                    let hash = bitcoin::BlockHash::from_byte_array(rec.hash);
                                    write_hub.is_archived(&hash)
                                }
                                _ => false,
                            };
                            if !archived {
                                break;
                            }
                            park.force_advance(1);
                            skipped += 1;
                        }
                        if skipped > 0 {
                            write_next.store(park.next_h(), Ordering::Relaxed);
                            ready = park.ready_prefix_len();
                            rbitcoin_log::debug!(
                                "ibd: ContigPark advanced past {skipped} already-archived height(s) next_h={}",
                                park.next_h()
                            );
                        }
                    }

                    // Wait briefly to grow a larger contiguous quanta when only a few
                    // heights at next_h are ready (same coalesce policy as before).
                    if ready > 0 && ready < min_batch {
                        let wait = coalesce_wait(
                            ready,
                            write_depth.load(Ordering::Relaxed),
                            write_arch_q.count(),
                            lag,
                        );
                        if !wait.is_zero() {
                            let deadline = Instant::now() + wait.min(Duration::from_millis(12));
                            while Instant::now() < deadline
                                && park.ready_prefix_len() < min_batch
                            {
                                match far_write_rx.recv_timeout(
                                    deadline.saturating_duration_since(Instant::now()),
                                ) {
                                    Ok(p) => {
                                        write_q_dec(&write_depth);
                                        if !handle_insert(
                                            &mut park,
                                            p,
                                            &write_hub,
                                            &write_result,
                                            park_horizon,
                                        ) {
                                            return;
                                        }
                                    }
                                    Err(_) => break,
                                }
                                while let Ok(p) = pri_write_rx.try_recv() {
                                    write_q_dec(&write_depth);
                                    if !handle_insert(
                                        &mut park,
                                        p,
                                        &write_hub,
                                        &write_result,
                                        park_horizon,
                                    ) {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    write_stats
                        .write_coalesce_ns
                        .fetch_add(coal_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    let batch = park.take_contiguous(max_mega);
                    if batch.is_empty() {
                        continue;
                    }

                    let n_blocks = batch.len() as u64;
                    let outcomes: Vec<(BlockHash, usize)> =
                        batch.iter().map(|p| (p.hash, p.wire_bytes)).collect();
                    let mut owned: Vec<_> = batch
                        .into_iter()
                        .map(|p| (p.header_fk, p.header, p.txs))
                        .collect();
                    let write_t0 = Instant::now();
                    let write_res = write_hub.query.archive_prepared_with_fks(&mut owned);
                    write_stats
                        .write_ns
                        .fetch_add(write_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    write_stats.write_batches.fetch_add(1, Ordering::Relaxed);
                    write_stats.write_blocks.fetch_add(n_blocks, Ordering::Relaxed);
                    write_stats
                        .write_batch_blocks
                        .fetch_add(n_blocks, Ordering::Relaxed);

                    match write_res {
                        Ok(_fks) => {
                            write_next.store(park.next_h(), Ordering::Relaxed);
                            for (hash, wire_bytes) in outcomes {
                                if write_result
                                    .send(ArchiveResult::Ok { hash, wire_bytes })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                            blocks_since_flush =
                                blocks_since_flush.saturating_add(n_blocks);
                            if blocks_since_flush >= FLUSH_EVERY_BLOCKS {
                                if let Err(e) = write_hub.query.flush_header_archive() {
                                    rbitcoin_log::warn!(
                                        "ibd: header archive flush failed: {e}"
                                    );
                                }
                                blocks_since_flush = 0;
                            }
                        }
                        Err(e) => {
                            // Contiguous run failed hard — report all; rewind HWM so
                            // re-getdata can rebuild the same heights.
                            let err = e.to_string();
                            let roll = outcomes.len() as u32;
                            park.rewind(roll);
                            write_next.store(park.next_h(), Ordering::Relaxed);
                            for (hash, wire_bytes) in outcomes {
                                if write_result
                                    .send(ArchiveResult::Err {
                                        hash,
                                        err: err.clone(),
                                        wire_bytes,
                                    })
                                    .is_err()
                                {
                                    return;
                                }
                            }
                        }
                    }
                }
            })
            .expect("spawn ibd-archive-writer");

        let (pri_prep_tx, pri_prep_rx) = std::sync::mpsc::sync_channel::<ArchiveJob>(PRI_Q);
        let (far_prep_tx, far_prep_rx) = std::sync::mpsc::sync_channel::<ArchiveJob>(WRITE_Q);
        let prep_params = hub.params.clone();
        let prep_stats = Arc::clone(&stats);
        let prep_result_tx = result_tx.clone();
        let prep_depth = Arc::clone(&write_q_depth);
        let prep_stop = Arc::clone(&stop);

        let prep_thread = std::thread::Builder::new()
            .name("ibd-archive-prep".into())
            .spawn(move || {
                let mut pri_open = true;
                let mut far_open = true;
                loop {
                    if prep_stop.load(Ordering::Relaxed) {
                        while pri_prep_rx.try_recv().is_ok() {}
                        while far_prep_rx.try_recv().is_ok() {}
                        break;
                    }
                    if !pri_open && !far_open {
                        break;
                    }
                    let job = match pri_prep_rx.try_recv() {
                        Ok(j) => j,
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            pri_open = false;
                            match far_prep_rx.recv() {
                                Ok(j) => j,
                                Err(_) => break,
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            match far_prep_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(j) => j,
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    match pri_prep_rx.recv() {
                                        Ok(j) => j,
                                        Err(_) => break,
                                    }
                                }
                            }
                        }
                    };
                    let hash = job.block.block_hash();
                    let header_fk = job.header_fk;
                    let priority = job.priority;
                    let wire_bytes = job.wire_bytes;
                    let height = job.height;
                    let prep_t0 = Instant::now();
                    let prep_res = prepare_block_for_archive_ibd(&prep_params, &job.block);
                    prep_stats
                        .prep_ns
                        .fetch_add(prep_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    prep_stats.prep_blocks.fetch_add(1, Ordering::Relaxed);
                    match prep_res {
                        Ok((header, txs)) => {
                            prep_depth.fetch_add(1, Ordering::Relaxed);
                            let item = PreparedArchive {
                                hash,
                                header,
                                txs,
                                header_fk,
                                wire_bytes,
                                height,
                            };
                            let send_ok = if priority {
                                pri_write_tx.send(item).is_ok()
                            } else {
                                far_write_tx.send(item).is_ok()
                            };
                            if !send_ok {
                                prep_depth.fetch_sub(1, Ordering::Relaxed);
                                let _ = prep_result_tx.send(ArchiveResult::Err {
                                    hash,
                                    err: "writer closed".into(),
                                    wire_bytes,
                                });
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = prep_result_tx.send(ArchiveResult::Err {
                                hash,
                                err: e.to_string(),
                                wire_bytes,
                            });
                        }
                    }
                }
            })
            .expect("spawn ibd-archive-prep");

        while !stop.load(Ordering::Relaxed) {
            let mut job = match tokio::time::timeout(Duration::from_millis(50), job_rx.recv()).await
            {
                Ok(Some(j)) => j,
                Ok(None) => break, // channel closed
                Err(_) => continue, // poll stop
            };
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let priority = job.priority;
            loop {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let tx = if priority {
                    &pri_prep_tx
                } else {
                    &far_prep_tx
                };
                match tx.try_send(job) {
                    Ok(()) => break,
                    Err(std::sync::mpsc::TrySendError::Full(j)) => {
                        job = j;
                        tokio::task::yield_now().await;
                        if !priority {
                            tokio::time::sleep(Duration::from_micros(200)).await;
                        } else {
                            tokio::time::sleep(Duration::from_micros(50)).await;
                        }
                    }
                    Err(std::sync::mpsc::TrySendError::Disconnected(j)) => {
                        let _ = result_tx.send(ArchiveResult::Err {
                            hash: j.block.block_hash(),
                            err: "prep closed".into(),
                            wire_bytes: j.wire_bytes,
                        });
                        break;
                    }
                }
            }
        }
        stop.store(true, Ordering::Relaxed);
        drop(pri_prep_tx);
        drop(far_prep_tx);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = prep_thread.join();
            let _ = writer.join();
        })
        .await;
    })
}

fn write_q_dec(depth: &AtomicUsize) {
    let _ = depth.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        Some(n.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn budget_default_is_512mib() {
        let b = ArchiveQueueBudget::new(DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES);
        assert_eq!(b.budget_bytes(), 512 * 1024 * 1024);
        assert!(b.fill_ratio() < 1.0);
        assert_eq!(b.count(), 0);
        assert_eq!(b.bytes(), 0);
    }

    #[test]
    fn far_scale_proportional_and_hysteresis() {
        // Empty → full far.
        let (s0, p0) = ArchiveQueueBudget::far_scale_from(0.0, false);
        assert!((s0 - 1.0).abs() < 1e-9 && !p0);
        // Half full → half far (proportional).
        let (s50, p50) = ArchiveQueueBudget::far_scale_from(0.5, false);
        assert!((s50 - 0.5).abs() < 1e-9 && !p50);
        // Enter pressure at 90%.
        let (s90, p90) = ArchiveQueueBudget::far_scale_from(0.90, false);
        assert_eq!(s90, 0.0);
        assert!(p90);
        // Stay in pressure until ≤70% even if fill eases to 80%.
        let (s80, p80) = ArchiveQueueBudget::far_scale_from(0.80, true);
        assert_eq!(s80, 0.0);
        assert!(p80);
        // Exit at 70% → proportional again.
        let (s70, p70) = ArchiveQueueBudget::far_scale_from(0.70, true);
        assert!((s70 - 0.30).abs() < 1e-9);
        assert!(!p70);
    }

    #[test]
    fn far_admission_scale_uses_live_bytes() {
        let budget = 100 * 1024 * 1024;
        let b = ArchiveQueueBudget::new(budget);
        // Charge to ~50% (charged_bytes applies 1.5× overhead).
        let wire = (budget * 2 / 3) / 2; // one charge → ~50% after overhead
        b.charge(wire);
        let fill = b.fill_ratio();
        assert!((0.4..0.6).contains(&fill), "fill={fill}");
        let scale = b.far_admission_scale();
        assert!((scale - (1.0 - fill)).abs() < 1e-9);
        assert!(!b.in_pressure());
        // Overshoot enter threshold.
        while b.fill_ratio() < ARCHIVE_PRESSURE_ENTER {
            b.charge(wire);
        }
        assert_eq!(b.far_admission_scale(), 0.0);
        assert!(b.in_pressure());
        // Drain to mid-band: still pressure.
        while b.fill_ratio() > 0.75 && b.count() > 0 {
            b.release(wire);
        }
        if b.fill_ratio() > ARCHIVE_PRESSURE_EXIT {
            assert_eq!(b.far_admission_scale(), 0.0);
            assert!(b.in_pressure());
        }
        // Drain to exit.
        while b.fill_ratio() > ARCHIVE_PRESSURE_EXIT && b.count() > 0 {
            b.release(wire);
        }
        let s = b.far_admission_scale();
        assert!(!b.in_pressure());
        assert!(s > 0.0, "scale after exit={s}");
    }

    #[test]
    fn charge_release_tracks_bytes_and_count() {
        let budget = 32 * 1024 * 1024;
        let b = ArchiveQueueBudget::new(budget);
        let w = 8 * 1024 * 1024;
        b.charge(w);
        b.charge(w);
        assert_eq!(b.count(), 2);
        assert_eq!(b.bytes(), ArchiveQueueBudget::charged_bytes(w) * 2);
        assert!(b.fill_ratio() < 1.0);
        b.charge(w);
        assert!(b.fill_ratio() >= 1.0);
        b.release(w);
        assert_eq!(b.count(), 2);
        assert!(b.fill_ratio() < 1.0);
        b.release(w);
        b.release(w);
        assert_eq!(b.count(), 0);
        assert_eq!(b.bytes(), 0);
    }

    #[test]
    fn try_charge_hard_caps_at_budget() {
        let budget = 32 * 1024 * 1024;
        let b = ArchiveQueueBudget::new(budget);
        // charged = wire×1.5 + 4 KiB → 4 MiB wire ≈ 6 MiB charged.
        let w = 4 * 1024 * 1024;
        assert!(b.try_charge(w));
        assert!(b.try_charge(w));
        assert!(b.try_charge(w));
        assert!(b.try_charge(w));
        assert!(b.try_charge(w)); // ~30 MiB charged of 32
        // Next would exceed — refuse without growing counters.
        let before_n = b.count();
        let before_b = b.bytes();
        assert!(!b.try_charge(w), "must refuse once at/over budget");
        assert_eq!(b.count(), before_n);
        assert_eq!(b.bytes(), before_b);
        // After release, admit again.
        b.release(w);
        assert!(b.try_charge(w));
    }

    #[test]
    fn try_charge_admits_first_even_if_block_exceeds_budget() {
        // Budget clamps to 16 MiB minimum; charge a wire size whose overhead > budget.
        let b = ArchiveQueueBudget::new(16 * 1024 * 1024);
        let huge = 32 * 1024 * 1024; // charged ≈ 48 MiB > 16 MiB
        assert!(b.try_charge(huge), "empty queue always admits one");
        assert_eq!(b.count(), 1);
        // Second must refuse.
        assert!(!b.try_charge(1));
        assert_eq!(b.count(), 1);
    }

    #[test]
    fn release_saturates_underflow() {
        let b = ArchiveQueueBudget::new(1024 * 1024);
        b.release(100);
        assert_eq!(b.count(), 0);
        assert_eq!(b.bytes(), 0);
    }

    #[test]
    fn tiny_budget_clamped_to_16mib() {
        let b = ArchiveQueueBudget::new(1);
        assert_eq!(b.budget_bytes(), 16 * 1024 * 1024);
    }
}
