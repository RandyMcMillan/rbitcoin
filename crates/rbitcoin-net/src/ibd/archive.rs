//! Archive prep + writer pipeline for IBD.

use super::coalesce::{coalesce_wait, max_batch_for_lag, min_batch_for_queue};
use crate::chain::ChainHub;
use bitcoin::hashes::Hash;
use bitcoin::{Block, BlockHash};
use rbitcoin_consensus::prepare_block_for_archive_ibd;
use rbitcoin_log::debug;
use rbitcoin_primitives::Fk;
use rbitcoin_query::TxApply;

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
/// (×1.5 charge); still stacked with parent-body mlock + page cache.
pub const DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES: usize = 512 * 1024 * 1024;

/// Enter “pressure” (far_scale = 0) when fill ≥ this fraction of budget.
pub const ARCHIVE_PRESSURE_ENTER: f64 = 0.90;
/// Leave pressure only after fill ≤ this (hysteresis vs enter).
pub const ARCHIVE_PRESSURE_EXIT: f64 = 0.70;

/// Shared counter of blocks (and approx wire bytes) in the archive pipeline.
///
/// Charged when a decoded body is handed to the job channel; released when the
/// writer (or prep error path) returns [`ArchiveResult`].
///
/// **Assign gate (not receive gate):** [`Self::can_assign`] is false once charged
/// fill ≥ budget — stop issuing new densify/cache getdata. Bodies already in
/// flight still [`charge`] and enqueue (may briefly overshoot). Soft
/// [`Self::far_admission_scale`] (proportional + 90%/70% hysteresis) scales
/// densify capacity before the hard stop.
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

    /// Charge a body entering the job channel (may overshoot budget).
    ///
    /// In-flight getdata must always be enqueued — never dump the first copy of
    /// a peer body because the meter is full. Cap **new** getdata via
    /// [`Self::can_assign`] instead.
    pub fn charge(&self, wire_bytes: usize) {
        let charged = Self::charged_bytes(wire_bytes);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(charged, Ordering::Relaxed);
    }

    /// Same overhead as [`charge`] — callers must release the charged amount.
    pub fn charged_bytes(wire_bytes: usize) -> usize {
        wire_bytes.saturating_mul(3).saturating_add(4096) / 2
    }

    /// True while charged fill is **strictly below** budget — issue densify /
    /// confirm-cache getdata. Tip-hole and ContigPark race assign ignore this
    /// so a hole at `write_next` can still be filled when the queue is full.
    pub fn can_assign(&self) -> bool {
        self.bytes() < self.budget
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

/// Plan+outcomes handed from prep (reads) to writer (writes only).
struct WriteReadyBatch {
    /// Release / accept results for every job in the batch (order preserved).
    outcomes: Vec<(BlockHash, usize)>,
    plan: rbitcoin_query::ArchiveWritePlan,
}

/// Parks **raw** archive jobs until a height-contiguous mega-batch is ready.
/// Prep (structure + FK resolve) runs only on contiguous prefixes — not on
/// out-of-order park fills.
struct ContigPark {
    /// Next height that may leave the park for prep/write (contiguous HWM + 1).
    next_h: u32,
    /// `height → job` for heights ≥ `next_h`.
    parked: BTreeMap<u32, ArchiveJob>,
}

/// Result of trying to park a job.
enum ParkInsert {
    /// Stored; wait for contiguous prefix.
    Parked,
    /// Height already past HWM or unknown — caller should single-path / fail.
    Late(ArchiveJob),
    /// Same height already parked (multi-peer redelivery). Caller must release
    /// the **second** charge via [`ArchiveResult::Dropped`] — **not** Ok.
    Duplicate(ArchiveJob),
    /// Too far ahead of `next_h` — refuse park so RAM stays near ContigPark head.
    BeyondHorizon(ArchiveJob),
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
    fn insert(&mut self, p: ArchiveJob, horizon: u32) -> ParkInsert {
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
    /// that height — e.g. Late path or resume HWM).
    fn force_advance(&mut self, n: u32) {
        if n == 0 {
            return;
        }
        self.next_h = self.next_h.saturating_add(n);
        while let Some((&h, _)) = self.parked.first_key_value() {
            if h < self.next_h {
                self.parked.remove(&h);
            } else {
                break;
            }
        }
    }

    /// Pop a contiguous run `[next_h, next_h+len)` of at most `max` blocks.
    /// Advances `next_h` by the number taken (caller must [`rewind`] on failure).
    fn take_contiguous(&mut self, max: usize) -> Vec<ArchiveJob> {
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

    /// Undo a failed mega-batch that already advanced `next_h`.
    fn rewind(&mut self, n: u32) {
        self.next_h = self.next_h.saturating_sub(n);
    }

    /// Drain all parked jobs (shutdown). Caller must release each charge.
    fn drain_all(&mut self) -> Vec<ArchiveJob> {
        std::mem::take(&mut self.parked).into_values().collect()
    }
}

#[cfg(test)]
mod contig_park_tests {
    use super::{ArchiveJob, ContigPark};
    use bitcoin::blockdata::block::Header as BlockHeader;
    use bitcoin::hashes::Hash;
    use bitcoin::{Block, BlockHash};
    use rbitcoin_primitives::Fk;

    fn job(h: u32) -> ArchiveJob {
        // Minimal empty block shell — ContigPark only cares about height.
        let header = BlockHeader {
            version: bitcoin::blockdata::block::Version::from_consensus(1),
            prev_blockhash: BlockHash::from_byte_array([0u8; 32]),
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time: 0,
            bits: bitcoin::CompactTarget::from_consensus(0),
            nonce: 0,
        };
        ArchiveJob {
            block: Block {
                header,
                txdata: vec![],
            },
            header_fk: Fk(h as u64 + 1),
            priority: false,
            wire_bytes: 100,
            height: h,
        }
    }

    #[test]
    fn parks_ahead_until_gap_filled() {
        let mut p = ContigPark::new(10);
        const H: u32 = 2048;
        assert!(matches!(p.insert(job(12), H), super::ParkInsert::Parked));
        assert!(matches!(p.insert(job(11), H), super::ParkInsert::Parked));
        assert!(p.take_contiguous(8).is_empty(), "gap at 10");
        assert_eq!(p.parked_len(), 2);
        assert!(matches!(p.insert(job(10), H), super::ParkInsert::Parked));
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
        match p.insert(job(3), 2048) {
            super::ParkInsert::Late(late) => assert_eq!(late.height, 3),
            _ => panic!("expected Late"),
        }
        assert_eq!(p.parked_len(), 0);
    }

    #[test]
    fn beyond_horizon_refused() {
        let mut p = ContigPark::new(10);
        match p.insert(job(10 + 2049), 2048) {
            super::ParkInsert::BeyondHorizon(f) => assert_eq!(f.height, 10 + 2049),
            _ => panic!("expected BeyondHorizon"),
        }
        assert_eq!(p.parked_len(), 0);
        assert!(matches!(p.insert(job(10 + 2048), 2048), super::ParkInsert::Parked));
    }

    #[test]
    fn duplicate_height_returns_dup_for_budget_release_only() {
        let mut p = ContigPark::new(1);
        assert!(matches!(p.insert(job(1), 2048), super::ParkInsert::Parked));
        match p.insert(job(1), 2048) {
            super::ParkInsert::Duplicate(d) => {
                assert_eq!(d.height, 1);
            }
            _ => panic!("expected Duplicate"),
        }
        assert_eq!(p.parked_len(), 1);
    }

    #[test]
    fn caps_run_at_max() {
        let mut p = ContigPark::new(0);
        for h in 0..10 {
            assert!(matches!(p.insert(job(h), 2048), super::ParkInsert::Parked));
        }
        let run = p.take_contiguous(4);
        assert_eq!(run.len(), 4);
        assert_eq!(p.next_h(), 4);
        assert_eq!(p.parked_len(), 6);
    }

    #[test]
    fn force_advance_unblocks_parked_prefix() {
        let mut p = ContigPark::new(10);
        assert!(matches!(p.insert(job(12), 2048), super::ParkInsert::Parked));
        assert!(matches!(p.insert(job(11), 2048), super::ParkInsert::Parked));
        assert!(p.take_contiguous(8).is_empty());
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

/// ContigPark → prep (structure + FK assign/resolve reads) → writer (Class A puts).
///
/// **Park first:** out-of-order wire jobs sit in ContigPark without Class A work.
/// When a contiguous prefix is ready, prep decodes TxApply and plans create_fks
/// (sticky + `tx.head` reads only). Writer commits the plan (body/head/header_txs
/// writes + sticky publish). Prep waits for write ack before planning the next
/// mega-batch so planned fks stay aligned with `txs.count()`.
pub(crate) fn spawn_archive_pipeline(
    hub: Arc<ChainHub>,
    mut job_rx: mpsc::UnboundedReceiver<ArchiveJob>,
    result_tx: mpsc::UnboundedSender<ArchiveResult>,
    stats: Arc<ArchivePipelineStats>,
    archive_queued: Arc<ArchiveQueueBudget>,
    confirm_lag: Arc<AtomicU32>,
    // Next height that may leave ContigPark (contiguous archived HWM + 1).
    write_next_height: Arc<AtomicU32>,
    // Cooperative stop (SIGINT): exit after current write batch; drop queue.
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        const JOB_Q: usize = 4096;
        const PRI_Q: usize = 512;
        debug!(
            "ibd: archive pipeline prep=1 (park+resolve) writer=1 (commit) contig-park-before-prep"
        );

        // Tokio → prep: raw jobs (priority jumps far).
        let (pri_job_tx, pri_job_rx) = std::sync::mpsc::sync_channel::<ArchiveJob>(PRI_Q);
        let (far_job_tx, far_job_rx) = std::sync::mpsc::sync_channel::<ArchiveJob>(JOB_Q);
        // Prep → writer: capacity 1 + blocking ack after every send (single-flight).
        // Must not leave prep with "in flight" while writer is idle (silent stall).
        let (write_tx, write_rx) = std::sync::mpsc::sync_channel::<WriteReadyBatch>(1);
        let (ack_tx, ack_rx) = std::sync::mpsc::sync_channel::<()>(1);

        let write_hub = hub.clone();
        let write_result = result_tx.clone();
        let write_stats = Arc::clone(&stats);
        let write_stop = Arc::clone(&stop);

        let writer = std::thread::Builder::new()
            .name("ibd-archive-writer".into())
            .spawn(move || {
                rbitcoin_store::try_set_io_idle();
                const FLUSH_EVERY_BLOCKS: u64 = 8192;
                let mut blocks_since_flush = 0u64;
                while !write_stop.load(Ordering::Relaxed) {
                    let idle_t0 = Instant::now();
                    let batch = match write_rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(b) => b,
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                            write_stats.write_idle_ns.fetch_add(
                                idle_t0.elapsed().as_nanos() as u64,
                                Ordering::Relaxed,
                            );
                            continue;
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                    };
                    write_stats.write_idle_ns.fetch_add(
                        idle_t0.elapsed().as_nanos() as u64,
                        Ordering::Relaxed,
                    );
                    let n_blocks = batch.outcomes.len() as u64;
                    let write_t0 = Instant::now();
                    let write_res = if batch.plan.is_empty() {
                        Ok(())
                    } else {
                        write_hub.query.archive_commit_plan(batch.plan)
                    };
                    write_stats
                        .write_ns
                        .fetch_add(write_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    write_stats.write_batches.fetch_add(1, Ordering::Relaxed);
                    write_stats
                        .write_blocks
                        .fetch_add(n_blocks, Ordering::Relaxed);
                    write_stats
                        .write_batch_blocks
                        .fetch_add(n_blocks, Ordering::Relaxed);

                    match write_res {
                        Ok(()) => {
                            for (hash, wire_bytes) in batch.outcomes {
                                if write_result
                                    .send(ArchiveResult::Ok { hash, wire_bytes })
                                    .is_err()
                                {
                                    let _ = ack_tx.send(());
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
                            let err = e.to_string();
                            for (hash, wire_bytes) in batch.outcomes {
                                if write_result
                                    .send(ArchiveResult::Err {
                                        hash,
                                        err: err.clone(),
                                        wire_bytes,
                                    })
                                    .is_err()
                                {
                                    let _ = ack_tx.send(());
                                    return;
                                }
                            }
                        }
                    }
                    // Unblock prep to plan the next mega-batch (fk HWM is durable).
                    let _ = ack_tx.send(());
                }
            })
            .expect("spawn ibd-archive-writer");

        let prep_hub = hub.clone();
        let prep_params = hub.params.clone();
        let prep_stats = Arc::clone(&stats);
        let prep_result = result_tx.clone();
        let prep_stop = Arc::clone(&stop);
        let prep_next = Arc::clone(&write_next_height);
        let prep_lag = Arc::clone(&confirm_lag);
        let prep_arch_q = Arc::clone(&archive_queued);

        let prep_thread = std::thread::Builder::new()
            .name("ibd-archive-prep".into())
            .spawn(move || {
                let mut pri_open = true;
                let mut far_open = true;
                let mut park = ContigPark::new(prep_next.load(Ordering::Relaxed));
                let park_horizon = super::CONTIG_DENSIFY_AHEAD;
                /// Emit Dropped / Err for park insert edge cases.
                fn handle_park_edge(
                    ins: ParkInsert,
                    result_tx: &mpsc::UnboundedSender<ArchiveResult>,
                    hub: &ChainHub,
                    write_tx: &std::sync::mpsc::SyncSender<WriteReadyBatch>,
                    ack_rx: &std::sync::mpsc::Receiver<()>,
                    stats: &ArchivePipelineStats,
                    params: &rbitcoin_consensus::ChainParams,
                ) -> bool {
                    match ins {
                        ParkInsert::Parked => true,
                        ParkInsert::Duplicate(dup) => result_tx
                            .send(ArchiveResult::Dropped {
                                hash: dup.block.block_hash(),
                                wire_bytes: dup.wire_bytes,
                                requeue: false,
                            })
                            .is_ok(),
                        ParkInsert::BeyondHorizon(far) => result_tx
                            .send(ArchiveResult::Dropped {
                                hash: far.block.block_hash(),
                                wire_bytes: far.wire_bytes,
                                requeue: true,
                            })
                            .is_ok(),
                        ParkInsert::Late(late) => {
                            // Already past HWM: plan+commit single without ContigPark.
                            !matches!(
                                plan_and_send_jobs(
                                    std::slice::from_ref(&late),
                                    hub,
                                    params,
                                    stats,
                                    write_tx,
                                    ack_rx,
                                    result_tx,
                                ),
                                PlanSend::WriterDead
                            )
                        }
                    }
                }

                /// Outcome of plan+blocking commit.
                enum PlanSend {
                    /// Commit finished (or empty batch / already archived Ok path).
                    Done,
                    /// Structure/plan failed; caller should rewind ContigPark HWM.
                    FailedRewind,
                    /// Writer channel dead.
                    WriterDead,
                }

                /// Structure decode + FK plan (reads), then **blocking** write+ack.
                ///
                /// Single-flight: does not return until the writer has finished
                /// commit (or failed). That keeps planned create fks aligned with
                /// `txs.count()` and prevents prep/writer desync stalls.
                fn plan_and_send_jobs(
                    jobs: &[ArchiveJob],
                    hub: &ChainHub,
                    params: &rbitcoin_consensus::ChainParams,
                    stats: &ArchivePipelineStats,
                    write_tx: &std::sync::mpsc::SyncSender<WriteReadyBatch>,
                    ack_rx: &std::sync::mpsc::Receiver<()>,
                    result_tx: &mpsc::UnboundedSender<ArchiveResult>,
                ) -> PlanSend {
                    if jobs.is_empty() {
                        return PlanSend::Done;
                    }

                    let prep_t0 = Instant::now();
                    let mut outcomes: Vec<(BlockHash, usize)> =
                        Vec::with_capacity(jobs.len());
                    let mut items: Vec<(Fk, rbitcoin_store::HeaderRecord, Vec<TxApply>)> =
                        Vec::with_capacity(jobs.len());

                    for job in jobs {
                        let hash = job.block.block_hash();
                        outcomes.push((hash, job.wire_bytes));
                        match prepare_block_for_archive_ibd(params, &job.block) {
                            Ok((header, txs)) => {
                                items.push((job.header_fk, header, txs));
                            }
                            Err(e) => {
                                let err = e.to_string();
                                for (h, wb) in &outcomes {
                                    let _ = result_tx.send(ArchiveResult::Err {
                                        hash: *h,
                                        err: err.clone(),
                                        wire_bytes: *wb,
                                    });
                                }
                                return PlanSend::FailedRewind;
                            }
                        }
                    }
                    stats
                        .prep_ns
                        .fetch_add(prep_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    stats
                        .prep_blocks
                        .fetch_add(jobs.len() as u64, Ordering::Relaxed);

                    let plan = match hub.query.archive_filter_need_bodies(&mut items) {
                        Ok((_fks, mut need)) => {
                            if need.is_empty() {
                                rbitcoin_query::ArchiveWritePlan::empty()
                            } else {
                                match hub.query.archive_plan_mega_owned(&mut need) {
                                    Ok(p) => p,
                                    Err(e) => {
                                        let err = e.to_string();
                                        for (h, wb) in &outcomes {
                                            let _ = result_tx.send(ArchiveResult::Err {
                                                hash: *h,
                                                err: err.clone(),
                                                wire_bytes: *wb,
                                            });
                                        }
                                        return PlanSend::FailedRewind;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let err = e.to_string();
                            for (h, wb) in &outcomes {
                                let _ = result_tx.send(ArchiveResult::Err {
                                    hash: *h,
                                    err: err.clone(),
                                    wire_bytes: *wb,
                                });
                            }
                            return PlanSend::FailedRewind;
                        }
                    };

                    // Empty plan still goes through writer so Ok results + ack stay ordered.
                    let batch = WriteReadyBatch { outcomes, plan };
                    if write_tx.send(batch).is_err() {
                        return PlanSend::WriterDead;
                    }
                    // Block until commit finishes — never leave "in flight" dangling.
                    match ack_rx.recv() {
                        Ok(()) => PlanSend::Done,
                        Err(_) => PlanSend::WriterDead,
                    }
                }

                loop {
                    if prep_stop.load(Ordering::Relaxed) {
                        while pri_job_rx.try_recv().is_ok() {}
                        while far_job_rx.try_recv().is_ok() {}
                        for j in park.drain_all() {
                            let _ = prep_result.send(ArchiveResult::Err {
                                hash: j.block.block_hash(),
                                err: "archive stopped".into(),
                                wire_bytes: j.wire_bytes,
                            });
                        }
                        break;
                    }
                    if !pri_open && !far_open && park.parked_len() == 0 {
                        break;
                    }

                    // Pull at least one job into the park.
                    let first = match pri_job_rx.try_recv() {
                        Ok(j) => Some(j),
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            pri_open = false;
                            match far_job_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(j) => Some(j),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    None
                                }
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            match far_job_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(j) => Some(j),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    match pri_job_rx.try_recv() {
                                        Ok(j) => Some(j),
                                        Err(std::sync::mpsc::TryRecvError::Empty) => None,
                                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                            pri_open = false;
                                            None
                                        }
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    match pri_job_rx.try_recv() {
                                        Ok(j) => Some(j),
                                        Err(_) => None,
                                    }
                                }
                            }
                        }
                    };

                    if let Some(j) = first {
                        let ins = park.insert(j, park_horizon);
                        if !handle_park_edge(
                            ins,
                            &prep_result,
                            &prep_hub,
                            &write_tx,
                            &ack_rx,
                            &prep_stats,
                            &prep_params,
                        ) {
                            return;
                        }
                    }

                    // Drain ready channels into park.
                    let coal_t0 = Instant::now();
                    loop {
                        let mut got = false;
                        while let Ok(j) = pri_job_rx.try_recv() {
                            let ins = park.insert(j, park_horizon);
                            if !handle_park_edge(
                                ins,
                                &prep_result,
                                &prep_hub,
                                &write_tx,
                                &ack_rx,
                                &prep_stats,
                                &prep_params,
                            ) {
                                return;
                            }
                            got = true;
                        }
                        while let Ok(j) = far_job_rx.try_recv() {
                            let ins = park.insert(j, park_horizon);
                            if !handle_park_edge(
                                ins,
                                &prep_result,
                                &prep_hub,
                                &write_tx,
                                &ack_rx,
                                &prep_stats,
                                &prep_params,
                            ) {
                                return;
                            }
                            got = true;
                        }
                        if !got {
                            break;
                        }
                    }

                    let lag = prep_lag.load(Ordering::Relaxed);
                    let arch_q_n = prep_arch_q.count();
                    let max_mega = max_batch_for_lag(lag);
                    let min_batch = min_batch_for_queue(arch_q_n, lag);
                    let mut ready = park.ready_prefix_len();
                    rbitcoin_query::contig_park_stats::store(
                        park.next_h(),
                        park.parked_len(),
                        ready,
                    );

                    // Skip already-Class-A heights blocking the park head.
                    if ready == 0 && park.parked_len() > 0 {
                        let mut skipped = 0u32;
                        while skipped < 64 && park.ready_prefix_len() == 0 {
                            let nh = park.next_h();
                            let archived = match prep_hub.query.header_at_height(
                                rbitcoin_primitives::Height(nh),
                            ) {
                                Ok(Some((_, rec))) => {
                                    let hash = bitcoin::BlockHash::from_byte_array(rec.hash);
                                    prep_hub.is_archived(&hash)
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
                            prep_next.store(park.next_h(), Ordering::Relaxed);
                            ready = park.ready_prefix_len();
                            rbitcoin_log::debug!(
                                "ibd: ContigPark advanced past {skipped} already-archived height(s) next_h={}",
                                park.next_h()
                            );
                        }
                    }

                    // Coalesce wait for larger contiguous quanta.
                    if ready > 0 && ready < min_batch {
                        let wait = coalesce_wait(ready, 0, prep_arch_q.count(), lag);
                        if !wait.is_zero() {
                            let deadline = Instant::now() + wait.min(Duration::from_millis(12));
                            while Instant::now() < deadline
                                && park.ready_prefix_len() < min_batch
                            {
                                match far_job_rx.recv_timeout(
                                    deadline.saturating_duration_since(Instant::now()),
                                ) {
                                    Ok(j) => {
                                        let ins = park.insert(j, park_horizon);
                                        if !handle_park_edge(
                                            ins,
                                            &prep_result,
                                            &prep_hub,
                                            &write_tx,
                                            &ack_rx,
                                            &prep_stats,
                                            &prep_params,
                                        ) {
                                            return;
                                        }
                                    }
                                    Err(_) => break,
                                }
                                while let Ok(j) = pri_job_rx.try_recv() {
                                    let ins = park.insert(j, park_horizon);
                                    if !handle_park_edge(
                                        ins,
                                        &prep_result,
                                        &prep_hub,
                                        &write_tx,
                                        &ack_rx,
                                        &prep_stats,
                                        &prep_params,
                                    ) {
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    prep_stats
                        .write_coalesce_ns
                        .fetch_add(coal_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    let batch = park.take_contiguous(max_mega);
                    if batch.is_empty() {
                        continue;
                    }
                    let n = batch.len() as u32;
                    // Publish park HWM for assign densify (may be ahead of arch_hwm
                    // until commit Ok marks archived).
                    prep_next.store(park.next_h(), Ordering::Relaxed);

                    // Blocks until writer commit+ack (single-flight).
                    match plan_and_send_jobs(
                        &batch,
                        &prep_hub,
                        &prep_params,
                        &prep_stats,
                        &write_tx,
                        &ack_rx,
                        &prep_result,
                    ) {
                        PlanSend::Done => {}
                        PlanSend::FailedRewind => {
                            park.rewind(n);
                            prep_next.store(park.next_h(), Ordering::Relaxed);
                        }
                        PlanSend::WriterDead => {
                            park.rewind(n);
                            prep_next.store(park.next_h(), Ordering::Relaxed);
                            return;
                        }
                    }
                }
                drop(write_tx);
            })
            .expect("spawn ibd-archive-prep");

        while !stop.load(Ordering::Relaxed) {
            let mut job = match tokio::time::timeout(Duration::from_millis(50), job_rx.recv()).await
            {
                Ok(Some(j)) => j,
                Ok(None) => break,
                Err(_) => continue,
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
                    &pri_job_tx
                } else {
                    &far_job_tx
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
        drop(pri_job_tx);
        drop(far_job_tx);
        let _ = tokio::task::spawn_blocking(move || {
            let _ = prep_thread.join();
            let _ = writer.join();
        })
        .await;
    })
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
    fn can_assign_stops_at_budget_charge_may_overshoot() {
        let budget = 32 * 1024 * 1024;
        let b = ArchiveQueueBudget::new(budget);
        // charged = wire×1.5 + 4 KiB → 4 MiB wire ≈ 6 MiB charged.
        let w = 4 * 1024 * 1024;
        assert!(b.can_assign());
        for _ in 0..5 {
            assert!(b.can_assign());
            b.charge(w);
        }
        // ~30 MiB < 32 → still assignable; one more overshoots meter but charge ok.
        assert!(b.can_assign());
        b.charge(w);
        assert!(!b.can_assign(), "fill ≥ budget → stop densify assign");
        // In-flight body still charges (overshoot) — never refuse receive.
        let before_n = b.count();
        b.charge(w);
        assert_eq!(b.count(), before_n + 1);
        assert!(b.bytes() > budget);
        b.release(w);
        b.release(w);
        assert!(b.can_assign());
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
