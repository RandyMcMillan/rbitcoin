//! Archive prep + writer pipeline for IBD.

use super::coalesce::{coalesce_wait, max_batch_for_lag, min_batch_for_lag};
use crate::chain::ChainHub;
use bitcoin::{Block, BlockHash};
use rbitcoin_consensus::prepare_block_for_archive_ibd;
use rbitcoin_log::debug;
use rbitcoin_primitives::Fk;
use rbitcoin_query::TxApply;
use rbitcoin_store::HeaderRecord;
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


/// Default RAM budget for decoded blocks waiting in the archive pipeline (~256 MiB).
/// Override with env `RBITCOIN_ARCHIVE_QUEUE_MB`.
///
/// Was 1 GiB; wire-size undercounts true RSS of decoded `Block` + prep structures,
/// and stacked with Class A cache + OS page cache of multi‑GB store files.
pub const DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES: usize = 256 * 1024 * 1024;

/// Shared counter of blocks (and approx wire bytes) in the archive pipeline.
///
/// Charged when a decoded body is handed to the job channel; released when the
/// writer (or prep error path) returns [`ArchiveResult`]. Used for getdata
/// backpressure so RAM waiting on archive stays near the configured budget.
pub(crate) struct ArchiveQueueBudget {
    count: AtomicUsize,
    bytes: AtomicUsize,
    budget: usize,
}

impl ArchiveQueueBudget {
    pub fn new(budget: usize) -> Self {
        Self {
            count: AtomicUsize::new(0),
            bytes: AtomicUsize::new(0),
            // At least 16 MiB so tiny overrides still leave room for a few blocks.
            budget: budget.max(16 * 1024 * 1024),
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

    /// True while charged bytes are under the budget (room for more getdata).
    pub fn has_room(&self) -> bool {
        self.bytes() < self.budget
    }

    /// Charge a block entering the pipeline (job channel → prep → writer).
    ///
    /// Applies a small overhead factor so RSS of decoded/`Prepared` structures
    /// is less likely to blow past the configured wire budget.
    pub fn charge(&self, wire_bytes: usize) {
        let charged = wire_bytes.saturating_mul(3).saturating_add(4096) / 2; // ×1.5 + 4 KiB
        self.count.fetch_add(1, Ordering::Relaxed);
        self.bytes.fetch_add(charged, Ordering::Relaxed);
    }

    /// Same overhead as [`charge`] — callers must release the charged amount.
    pub fn charged_bytes(wire_bytes: usize) -> usize {
        wire_bytes.saturating_mul(3).saturating_add(4096) / 2
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
}

struct PreparedArchive {
    hash: BlockHash,
    header: HeaderRecord,
    txs: Vec<TxApply>,
    header_fk: Fk,
    priority: bool,
    wire_bytes: usize,
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
}

/// Dual-lane prep + writer: tip-near priority jumps far FIFO.
///
/// Archive never stalls for confirm: long `confirm_live` waves used to starve the
/// far lane, fill `arch_q`, freeze getdata (`inflight=0`), then false peer stall
/// disconnects. Priority still wins when both lanes have work.
pub(crate) fn spawn_archive_pipeline(
    hub: Arc<ChainHub>,
    mut job_rx: mpsc::UnboundedReceiver<ArchiveJob>,
    result_tx: mpsc::UnboundedSender<ArchiveResult>,
    stats: Arc<ArchivePipelineStats>,
    archive_queued: Arc<ArchiveQueueBudget>,
    confirm_lag: Arc<AtomicU32>,
    // Cooperative stop (SIGINT): exit after current write batch; drop queue.
    stop: Arc<AtomicBool>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        const WRITE_Q: usize = 4096;
        const PRI_Q: usize = 512;
        debug!("ibd: archive pipeline prep=1 (OS thread) writer=1 (OS thread) dual-lane");

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
        let write_stop = Arc::clone(&stop);

        let writer = std::thread::Builder::new()
            .name("ibd-archive-writer".into())
            .spawn(move || {
                rbitcoin_store::try_set_io_idle();
                // Batch size is lag-aware (see coalesce::max_batch_for_lag): when
                // Class A leads tip deeply, use **small** quanta so confirm can
                // interleave instead of multi-minute 300–1024 block dumps.
                const FLUSH_EVERY_BLOCKS: u64 = 8192;
                let mut blocks_since_flush = 0u64;
                let mut pri_open = true;
                let mut far_open = true;
                loop {
                    if write_stop.load(Ordering::Relaxed) {
                        // Drop queued prepared work so prep can exit; skip writes.
                        while pri_write_rx.try_recv().is_ok() {
                            write_q_dec(&write_depth);
                        }
                        while far_write_rx.try_recv().is_ok() {
                            write_q_dec(&write_depth);
                        }
                        break;
                    }
                    if !pri_open && !far_open {
                        break;
                    }
                    let idle_t0 = Instant::now();
                    // Priority first; otherwise drain far. Never idle while either
                    // lane has work (confirm runs on a separate OS thread).
                    let (first, is_pri) = match pri_write_rx.try_recv() {
                        Ok(p) => (p, true),
                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                            pri_open = false;
                            match far_write_rx.recv() {
                                Ok(p) => (p, false),
                                Err(_) => break,
                            }
                        }
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            match far_write_rx.recv_timeout(Duration::from_millis(2)) {
                                Ok(p) => (p, false),
                                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                                    match pri_write_rx.try_recv() {
                                        Ok(p) => (p, true),
                                        Err(std::sync::mpsc::TryRecvError::Empty) => continue,
                                        Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                            pri_open = false;
                                            continue;
                                        }
                                    }
                                }
                                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                                    far_open = false;
                                    match pri_write_rx.recv() {
                                        Ok(p) => (p, true),
                                        Err(_) => break,
                                    }
                                }
                            }
                        }
                    };
                    write_q_dec(&write_depth);
                    write_stats
                        .write_idle_ns
                        .fetch_add(idle_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

                    let lag = write_lag.load(Ordering::Relaxed);
                    let max_mega = max_batch_for_lag(lag);
                    // Tip-near priority: always small so confirm runway lands fast.
                    let pri_cap = 32usize.min(max_mega);

                    let coal_t0 = Instant::now();
                    let mut batch = Vec::with_capacity(if is_pri { pri_cap } else { max_mega });
                    batch.push(first);
                    if is_pri {
                        while batch.len() < pri_cap {
                            match pri_write_rx.try_recv() {
                                Ok(p) => {
                                    write_q_dec(&write_depth);
                                    batch.push(p);
                                }
                                Err(_) => break,
                            }
                        }
                    } else {
                        let min_batch = min_batch_for_lag(lag);
                        while batch.len() < max_mega {
                            // Tip-near always wins mid far-pack.
                            if let Ok(p) = pri_write_rx.try_recv() {
                                write_q_dec(&write_depth);
                                batch.insert(0, p);
                                break;
                            }
                            match far_write_rx.try_recv() {
                                Ok(p) => {
                                    write_q_dec(&write_depth);
                                    batch.push(p);
                                }
                                Err(_) => break,
                            }
                        }
                        if batch.len() < min_batch && batch.iter().all(|p| !p.priority) {
                            let wait = coalesce_wait(
                                batch.len(),
                                write_depth.load(Ordering::Relaxed),
                                write_arch_q.count(),
                                lag,
                            );
                            if !wait.is_zero() {
                                let deadline = Instant::now() + wait;
                                while batch.len() < max_mega {
                                    if Instant::now() >= deadline {
                                        break;
                                    }
                                    if let Ok(p) = pri_write_rx.try_recv() {
                                        write_q_dec(&write_depth);
                                        batch.insert(0, p);
                                        break;
                                    }
                                    match far_write_rx.recv_timeout(
                                        deadline.saturating_duration_since(Instant::now()),
                                    ) {
                                        Ok(p) => {
                                            write_q_dec(&write_depth);
                                            batch.push(p);
                                        }
                                        Err(_) => break,
                                    }
                                }
                            }
                        }
                    }
                    write_stats
                        .write_coalesce_ns
                        .fetch_add(coal_t0.elapsed().as_nanos() as u64, Ordering::Relaxed);

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
                            // Deep confirm lag: yield disk so wave parent loads
                            // are not starved (mainnet ~220k tip freeze while
                            // arch lead ~400k and writer_busy%=100).
                            if !is_pri && lag >= 4_096 {
                                let steps = (lag / 4_096).min(25);
                                let ms = (steps as u64).saturating_mul(8); // 8..200ms
                                std::thread::sleep(Duration::from_millis(ms));
                            }
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
                            let err = e.to_string();
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
                                priority,
                                wire_bytes,
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
    fn budget_default_is_256mib() {
        let b = ArchiveQueueBudget::new(DEFAULT_ARCHIVE_QUEUE_BUDGET_BYTES);
        assert_eq!(b.budget_bytes(), 256 * 1024 * 1024);
        assert!(b.has_room());
        assert_eq!(b.count(), 0);
        assert_eq!(b.bytes(), 0);
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
        assert!(b.has_room());
        b.charge(w);
        assert!(!b.has_room());
        b.release(w);
        assert_eq!(b.count(), 2);
        assert!(b.has_room());
        b.release(w);
        b.release(w);
        assert_eq!(b.count(), 0);
        assert_eq!(b.bytes(), 0);
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
