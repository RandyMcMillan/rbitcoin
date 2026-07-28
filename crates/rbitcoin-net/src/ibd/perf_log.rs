//! Consolidated IBD performance sampling and logging.
//!
//! **Cadence:** one centralized ~5s status tick (see `ibd` main loop) emits
//! `ibd: progress`, `ibd: perf`, and `ibd: sizes` together.
//!
//! | Level | Message | Contents |
//! |-------|---------|----------|
//! | INFO  | `ibd: progress …` | Tip rate over the **last 5s**, loadq/writeq, txs=, horizon, tip ETA, durable `bq=` |
//! | INFO  | `ibd: perf …` | Download + durable queue pressure; **load / script / write** stage walls with load+write sub-phases; pin mix; queues |
//! | INFO  | `ibd: sizes …` | RSS + work path + arch RAM queue + **bq** + residency + outfifo + conf pipe + tx.head |
//! | DEBUG | `ibd: perf_dbg …` | µs/blk, pin/edge detail, archive Class A prep/write pipe, contig park |
//!
//! Sample **once** per tick and reset all atomics, then format INFO always and
//! DEBUG only when enabled — so DEBUG never sees an empty window after INFO.
//!
//! Schema 12: more wall time sits in confirm **load** (Class A decode + parent
//! pin / residency / denserels) and **write** (structural / Class C / spend
//! annotate). INFO breaks those stages out so bottlenecks are visible without
//! DEBUG. Formatters omit ghost columns from deleted paths.

use super::archive::{ArchivePipelineSample, ArchivePipelineStats};
use super::confirm::ConfirmPipelineSizes;
use super::state::WorkStructureSizes;
use super::status::LoopStats;
use rbitcoin_log::{debug, enabled, info, Level};
use rbitcoin_query::ProcessOwnedSizes;

/// One 5s window of IBD counters (post sample-and-reset).
#[derive(Clone, Debug)]
pub(crate) struct IbdPerfSample {
    // Pipeline health (not from atomics).
    pub inflight: usize,
    pub inflight_cap: usize,
    /// RAM archive pipeline queue (decoded jobs waiting for Class A write).
    pub arch_q: usize,
    pub arch_mb: usize,
    pub arch_budget_mb: usize,
    /// Durable on-disk block queue (schema 12): budget/used bytes + entry count.
    pub bq_budget: u64,
    pub bq_bytes: u64,
    pub bq_count: usize,
    pub pending: usize,
    /// Class A HWM (+ inflight) ahead of tip — densify / download headroom, not progress lead.
    pub arch_ahead: u32,
    pub hole: usize,
    pub peers: usize,
    pub headers_done: bool,

    // LoopStats
    pub confirm_ms: u64,
    pub confirm_blocks: u64,
    pub confirm_reject_stops: u64,
    pub confirm_us_per_block: u64,
    pub assign_ms: u64,
    pub assign_issued: u64,
    pub drain_ms: u64,
    pub drain_events: u64,
    pub status_scan_ms: u64,
    pub dominant: &'static str,
    /// `(first, batch_n, elapsed_ms)` if confirm mid-batch.
    pub live: Option<(u32, u32, u64)>,

    // Confirm phases (ns → ms at format)
    pub phase_blks: u64,
    pub recon_ms: u64,
    pub wire_ms: u64,
    pub connect_ms: u64,
    pub script_ms: u64,
    pub class_c_ms: u64,
    pub strong_ms: u64,
    pub sh_ms: u64,
    /// Post–Class C durable spend annotate wall (logged as `spend=` ms).
    pub utxo_ms: u64,
    /// Write structural total (spentness+maturity+BIP68+subsidy); not load `connect`.
    pub structural_ms: u64,
    /// Structural sub: durable spentness probes.
    pub structural_spent_ms: u64,
    /// Structural sub: create-height + coinbase maturity.
    pub structural_create_h_ms: u64,
    /// Structural sub: BIP68 + coin MTP.
    pub structural_bip68_ms: u64,
    pub spend_ranged: u64,
    pub spend_idx: u64,
    pub spend_skip: u64,
    pub resolve_ms: u64,
    pub load_ms: u64,
    pub cache_tip_ms: u64,
    // raw ns for us/blk
    pub recon_ns: u64,
    pub wire_ns: u64,
    pub connect_ns: u64,
    pub script_ns: u64,
    pub class_c_ns: u64,
    pub strong_ns: u64,
    pub sh_ns: u64,
    pub tip_ns: u64,
    pub utxo_apply_ns: u64,
    pub structural_ns: u64,
    pub structural_spent_ns: u64,
    pub structural_create_h_ns: u64,
    pub structural_bip68_ns: u64,
    pub resolve_ns: u64,
    pub load_ns: u64,
    pub cache_tip_ns: u64,

    pub sh_runs: usize,

    /// Wire rebuild: store body decode count + wall ms.
    pub wf_body_store: u64,
    pub wf_store_body_ms: u64,

    // SH sub (Direct: collect; tip append: sort/seed/body/head)
    pub sh_filter_ms: u64,
    pub sh_collect_ms: u64,
    pub sh_sort_ms: u64,
    pub sh_seed_ms: u64,
    pub sh_body_ms: u64,
    pub sh_head_ms: u64,

    // Parent cache / confirm-load
    pub load_win_ms: u64,
    pub load_blocks: u64,
    pub load_utxo_parents: u64,
    pub load_creates: u64,
    pub load_parent_unique: u64,
    pub load_pin_cache_body: u64,
    /// Pin hits from CreateResidency (subset of pin_cache when residency filled).
    pub load_pin_residency: u64,
    pub load_pin_new: u64,
    pub load_pin_spent_ms: u64,
    pub load_pin_body_ms: u64,
    pub load_pin_new_meta_ms: u64,
    pub load_body_tx_reads: u64,
    pub load_parent_tx_reads: u64,
    pub load_missing_parents: u64,
    pub load_ready_through: u32,
    pub cache_bodies: usize,
    pub cache_plans: usize,
    pub conf_load_q: usize,
    pub conf_write_q: usize,
    pub conf_load_q_cap: usize,
    pub conf_write_q_cap: usize,
    pub load_hdr_ms: u64,
    pub load_decode_ms: u64,
    pub load_thin_ms: u64,
    pub load_parent_pin_ms: u64,
    pub load_cache_put_ms: u64,
    pub load_edge_same: u64,
    pub load_edge_fk: u64,
    pub load_edge_cb: u64,

    // Archive
    pub arch_ext_need: u64,
    pub arch_sticky_hit: u64,
    pub arch_head_need: u64,
    pub arch_head_hit: u64,
    pub arch_batch_stamp: u64,
    pub arch_resolved_stamp: u64,
    pub arch_resolve_ns: u64,
    pub arch_resolve_blocks: u64,
    pub arch_prep_total_ms: u64,
    pub arch_prep_struct_ms: u64,
    pub arch_prep_filter_ms: u64,
    pub arch_prep_assign_ms: u64,
    pub arch_prep_collect_ms: u64,
    pub arch_prep_sticky_ms: u64,
    pub arch_prep_inflight_ms: u64,
    pub arch_prep_head_ms: u64,
    pub arch_prep_probe_ms: u64,
    pub arch_prep_idx_ms: u64,
    pub arch_prep_body_txid_ms: u64,
    pub arch_prep_head_keys: u64,
    pub arch_prep_head_cands: u64,
    pub arch_prep_body_lookups: u64,
    pub arch_prep_stamp_ms: u64,
    pub arch_prep_finish_ms: u64,
    pub arch_prep_publish_ms: u64,
    pub arch_prep_qwait_ms: u64,
    pub arch_prep_blocks: u64,
    pub arch_write_total_ms: u64,
    pub arch_write_reserve_ms: u64,
    pub arch_write_body_ms: u64,
    pub arch_write_head_ms: u64,
    pub arch_write_spend_ms: u64,
    pub arch_write_htxs_ms: u64,
    pub arch_write_sticky_ms: u64,
    pub arch_write_dontneed_ms: u64,
    pub arch_write_flush_ms: u64,
    pub arch_write_blocks: u64,
    pub arch_sticky_len: usize,
    pub arch_sticky_cap: usize,

    pub contig_next_h: u32,
    pub contig_parked: usize,
    pub contig_ready: usize,

    pub pipe: ArchivePipelineSample,

    /// Process RSS / smaps (kB); 0 when `/proc` unavailable.
    pub rss_kb: u64,
    pub rss_anon_kb: u64,
    pub rss_file_kb: u64,
    pub vm_hwm_kb: u64,
    /// `mlock`ed pages only (usually 0); RSS is **not** limited to these.
    pub rss_locked_kb: u64,
    /// Work-path + body presence occupancy (O(1) lens).
    pub work: WorkStructureSizes,
    /// Query-side process-owned caches (OutFifo, sticky, SH, tx.head resize).
    pub owned: ProcessOwnedSizes,
    /// Confirm load/scripts/write queue contents + feed.
    pub conf_pipe: ConfirmPipelineSizes,
}

impl Default for IbdPerfSample {
    fn default() -> Self {
        Self {
            inflight: 0,
            inflight_cap: 0,
            arch_q: 0,
            arch_mb: 0,
            arch_budget_mb: 0,
            bq_budget: 0,
            bq_bytes: 0,
            bq_count: 0,
            pending: 0,
            arch_ahead: 0,
            hole: 0,
            peers: 0,
            headers_done: false,
            confirm_ms: 0,
            confirm_blocks: 0,
            confirm_reject_stops: 0,
            confirm_us_per_block: 0,
            assign_ms: 0,
            assign_issued: 0,
            drain_ms: 0,
            drain_events: 0,
            status_scan_ms: 0,
            dominant: "idle",
            live: None,
            phase_blks: 0,
            recon_ms: 0,
            wire_ms: 0,
            connect_ms: 0,
            script_ms: 0,
            class_c_ms: 0,
            strong_ms: 0,
            sh_ms: 0,
            utxo_ms: 0,
            structural_ms: 0,
            structural_spent_ms: 0,
            structural_create_h_ms: 0,
            structural_bip68_ms: 0,
            spend_ranged: 0,
            spend_idx: 0,
            spend_skip: 0,
            resolve_ms: 0,
            load_ms: 0,
            cache_tip_ms: 0,
            recon_ns: 0,
            wire_ns: 0,
            connect_ns: 0,
            script_ns: 0,
            class_c_ns: 0,
            strong_ns: 0,
            sh_ns: 0,
            tip_ns: 0,
            utxo_apply_ns: 0,
            structural_ns: 0,
            structural_spent_ns: 0,
            structural_create_h_ns: 0,
            structural_bip68_ns: 0,
            resolve_ns: 0,
            load_ns: 0,
            cache_tip_ns: 0,
            sh_runs: 0,
            wf_body_store: 0,
            wf_store_body_ms: 0,
            sh_filter_ms: 0,
            sh_collect_ms: 0,
            sh_sort_ms: 0,
            sh_seed_ms: 0,
            sh_body_ms: 0,
            sh_head_ms: 0,
            load_win_ms: 0,
            load_blocks: 0,
            load_utxo_parents: 0,
            load_creates: 0,
            load_parent_unique: 0,
            load_pin_cache_body: 0,
            load_pin_residency: 0,
            load_pin_new: 0,
            load_pin_spent_ms: 0,
            load_pin_body_ms: 0,
            load_pin_new_meta_ms: 0,
            load_body_tx_reads: 0,
            load_parent_tx_reads: 0,
            load_missing_parents: 0,
            load_ready_through: 0,
            cache_bodies: 0,
            cache_plans: 0,
            conf_load_q: 0,
            conf_write_q: 0,
            conf_load_q_cap: super::confirm::LOAD_QUEUE_CAP,
            conf_write_q_cap: super::confirm::WRITE_QUEUE_CAP,
            load_hdr_ms: 0,
            load_decode_ms: 0,
            load_thin_ms: 0,
            load_parent_pin_ms: 0,
            load_cache_put_ms: 0,
            load_edge_same: 0,
            load_edge_fk: 0,
            load_edge_cb: 0,
            arch_ext_need: 0,
            arch_sticky_hit: 0,
            arch_head_need: 0,
            arch_head_hit: 0,
            arch_batch_stamp: 0,
            arch_resolved_stamp: 0,
            arch_resolve_ns: 0,
            arch_resolve_blocks: 0,
            arch_prep_total_ms: 0,
            arch_prep_struct_ms: 0,
            arch_prep_filter_ms: 0,
            arch_prep_assign_ms: 0,
            arch_prep_collect_ms: 0,
            arch_prep_sticky_ms: 0,
            arch_prep_inflight_ms: 0,
            arch_prep_head_ms: 0,
            arch_prep_probe_ms: 0,
            arch_prep_idx_ms: 0,
            arch_prep_body_txid_ms: 0,
            arch_prep_head_keys: 0,
            arch_prep_head_cands: 0,
            arch_prep_body_lookups: 0,
            arch_prep_stamp_ms: 0,
            arch_prep_finish_ms: 0,
            arch_prep_publish_ms: 0,
            arch_prep_qwait_ms: 0,
            arch_prep_blocks: 0,
            arch_write_total_ms: 0,
            arch_write_reserve_ms: 0,
            arch_write_body_ms: 0,
            arch_write_head_ms: 0,
            arch_write_spend_ms: 0,
            arch_write_htxs_ms: 0,
            arch_write_sticky_ms: 0,
            arch_write_dontneed_ms: 0,
            arch_write_flush_ms: 0,
            arch_write_blocks: 0,
            arch_sticky_len: 0,
            arch_sticky_cap: 0,
            contig_next_h: 0,
            contig_parked: 0,
            contig_ready: 0,
            pipe: ArchivePipelineSample::default(),
            rss_kb: 0,
            rss_anon_kb: 0,
            rss_file_kb: 0,
            vm_hwm_kb: 0,
            rss_locked_kb: 0,
            work: WorkStructureSizes::default(),
            owned: ProcessOwnedSizes::default(),
            conf_pipe: ConfirmPipelineSizes::default(),
        }
    }
}

/// Process memory from `/proc` (Linux). All fields kB; zeros if unavailable.
///
/// **RSS includes all resident pages**, not only `mlock`ed ones. Ordinary
/// anonymous heap and **file-backed mmap pages that have been faulted in**
/// (e.g. store `tx.head` / table maps) both count toward VmRSS until the kernel
/// reclaims them under pressure (or `MADV_DONTNEED` / unmap).
///
/// Split:
/// - `anon_kb` — process-private anonymous (heap, stacks, MAP_ANON)
/// - `file_kb` — file-backed resident (shared libs + **our table mmaps**)
/// - `locked_kb` — `mlock`/`mlockall` only (usually 0 for us)
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ProcRss {
    pub rss_kb: u64,
    pub anon_kb: u64,
    pub file_kb: u64,
    pub hwm_kb: u64,
    /// Pages locked into RAM (`Locked:` / mlock). Not required for RSS membership.
    pub locked_kb: u64,
}

/// Cheap once-per-tick `/proc` read (not hot path).
///
/// Prefer `/proc/self/status` fields present on modern kernels (`RssAnon` /
/// `RssFile` / `VmRSS`). Fall back to `smaps_rollup` (`Anonymous:`, `Rss:`,
/// `Locked:`) when status split is missing — older rollups do **not** expose
/// `RssAnon:` / `RssFile:` (that bug made `ibd: sizes` print `anon=0 file=0`).
pub(crate) fn read_proc_rss() -> ProcRss {
    let mut out = ProcRss::default();
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmRSS:") {
                out.rss_kb = parse_kb_field(rest);
            } else if let Some(rest) = line.strip_prefix("VmHWM:") {
                out.hwm_kb = parse_kb_field(rest);
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
                out.anon_kb = parse_kb_field(rest);
            } else if let Some(rest) = line.strip_prefix("RssFile:") {
                out.file_kb = parse_kb_field(rest);
            } else if let Some(rest) = line.strip_prefix("RssShmem:") {
                // Shmem is neither classic anon nor file-backed table mmap; fold
                // into file for operator "not heap" view if present.
                let sh = parse_kb_field(rest);
                if sh > 0 {
                    out.file_kb = out.file_kb.saturating_add(sh);
                }
            }
        }
    }
    if let Ok(s) = std::fs::read_to_string("/proc/self/smaps_rollup") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("Rss:") {
                if out.rss_kb == 0 {
                    out.rss_kb = parse_kb_field(rest);
                }
            } else if let Some(rest) = line.strip_prefix("Anonymous:") {
                // Rollup name when status RssAnon missing.
                if out.anon_kb == 0 {
                    out.anon_kb = parse_kb_field(rest);
                }
            } else if let Some(rest) = line.strip_prefix("RssAnon:") {
                if out.anon_kb == 0 {
                    out.anon_kb = parse_kb_field(rest);
                }
            } else if let Some(rest) = line.strip_prefix("RssFile:") {
                if out.file_kb == 0 {
                    out.file_kb = parse_kb_field(rest);
                }
            } else if let Some(rest) = line.strip_prefix("Locked:") {
                out.locked_kb = parse_kb_field(rest);
            }
        }
        // If we have RSS + anon but no file split, residual is file-backed.
        if out.file_kb == 0 && out.rss_kb > 0 && out.anon_kb > 0 && out.anon_kb <= out.rss_kb {
            out.file_kb = out.rss_kb.saturating_sub(out.anon_kb);
        }
    }
    out
}

fn parse_kb_field(rest: &str) -> u64 {
    rest.split_whitespace()
        .next()
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn kb_mib(kb: u64) -> u64 {
    kb / 1024
}

/// Sample every counter once and reset atomics.
pub(crate) fn sample(
    loop_stats: &LoopStats,
    pipe_stats: &ArchivePipelineStats,
    inflight: usize,
    inflight_cap: usize,
    arch_q: usize,
    arch_mb: usize,
    arch_budget_mb: usize,
    // Durable block queue: (budget_bytes, used_bytes, count).
    bq: (u64, u64, usize),
    pending: usize,
    _known_arch: usize,
    _ordered: usize,
    arch_ahead: u32,
    hole: usize,
    peers: usize,
    headers_done: bool,
    // (ready_through, ahead, parents, bodies, plans).
    load: (u32, u32, usize, usize, usize),
    conf_load_q: usize,
    conf_write_q: usize,
    sh_runs: usize,
    // Live archive sticky (len, cap).
    arch_sticky: (usize, usize),
    work: WorkStructureSizes,
    owned: ProcessOwnedSizes,
    conf_pipe: ConfirmPipelineSizes,
    rss: ProcRss,
) -> IbdPerfSample {
    let (bq_budget, bq_bytes, bq_count) = bq;
    let hot = loop_stats.sample_and_reset();
    let (
        recon_ns,
        wire_ns,
        connect_ns,
        script_ns,
        class_c_ns,
        strong_ns,
        sh_ns,
        tip_ns,
        utxo_apply_ns,
        phase_blks,
        resolve_ns,
        load_ns,
        _unpin_ns,
        cache_tip_ns,
        spend_ranged,
        spend_idx,
        spend_skip,
        structural_ns,
        structural_spent_ns,
        structural_create_h_ns,
        structural_bip68_ns,
    ) = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    let (sh_filter, sh_collect, sh_sort, sh_seed, sh_body, sh_head) =
        rbitcoin_query::class_c_phase_stats::sample_sh_sub_and_reset();
    let (wf_body_store, wf_store_body_ns) =
        rbitcoin_query::wave_fill_stats::sample_store_and_reset();
    // Drain connect prevout counters (not displayed; avoid unbounded growth).
    let _ = rbitcoin_query::connect_prevout_stats::sample_and_reset();
    let pw = rbitcoin_query::confirm_load_stats::sample_and_reset();
    let arch_res = rbitcoin_query::archive_phase_stats::sample_and_reset();
    let head_res = rbitcoin_store::head_resolve_stats::sample_and_reset();
    let pipe = pipe_stats.sample_and_reset();
    let (contig_next_h, contig_parked, contig_ready) =
        rbitcoin_query::contig_park_stats::snapshot();
    let (load_ready_through, _cache_ahead, _cache_parents, cache_bodies, cache_plans) = load;
    let (arch_sticky_len, arch_sticky_cap) = arch_sticky;

    IbdPerfSample {
        inflight,
        inflight_cap,
        arch_q,
        arch_mb,
        arch_budget_mb,
        bq_budget,
        bq_bytes,
        bq_count,
        pending,
        arch_ahead,
        hole,
        peers,
        headers_done,
        confirm_ms: hot.confirm_ms(),
        confirm_blocks: hot.confirm_blocks,
        confirm_reject_stops: hot.confirm_reject_stops,
        confirm_us_per_block: hot.confirm_us_per_block(),
        assign_ms: hot.assign_ms(),
        assign_issued: hot.assign_issued,
        drain_ms: hot.drain_ms(),
        drain_events: hot.drain_events,
        status_scan_ms: hot.status_scan_ms(),
        dominant: hot.dominant(),
        live: hot.confirm_live,
        phase_blks,
        recon_ms: ns_ms(recon_ns),
        wire_ms: ns_ms(wire_ns),
        connect_ms: ns_ms(connect_ns),
        script_ms: ns_ms(script_ns),
        class_c_ms: ns_ms(class_c_ns),
        strong_ms: ns_ms(strong_ns),
        sh_ms: ns_ms(sh_ns),
        utxo_ms: ns_ms(utxo_apply_ns),
        structural_ms: ns_ms(structural_ns),
        structural_spent_ms: ns_ms(structural_spent_ns),
        structural_create_h_ms: ns_ms(structural_create_h_ns),
        structural_bip68_ms: ns_ms(structural_bip68_ns),
        spend_ranged,
        spend_idx,
        spend_skip,
        resolve_ms: ns_ms(resolve_ns),
        load_ms: ns_ms(load_ns),
        cache_tip_ms: ns_ms(cache_tip_ns),
        recon_ns,
        wire_ns,
        connect_ns,
        script_ns,
        class_c_ns,
        strong_ns,
        sh_ns,
        tip_ns,
        utxo_apply_ns,
        structural_ns,
        structural_spent_ns,
        structural_create_h_ns,
        structural_bip68_ns,
        resolve_ns,
        load_ns,
        cache_tip_ns,
        sh_runs,
        wf_body_store,
        wf_store_body_ms: ns_ms(wf_store_body_ns),
        sh_filter_ms: ns_ms(sh_filter),
        sh_collect_ms: ns_ms(sh_collect),
        sh_sort_ms: ns_ms(sh_sort),
        sh_seed_ms: ns_ms(sh_seed),
        sh_body_ms: ns_ms(sh_body),
        sh_head_ms: ns_ms(sh_head),
        load_win_ms: ns_ms(pw.ns),
        load_blocks: pw.blocks,
        load_utxo_parents: pw.utxo_parents,
        load_creates: pw.creates,
        load_parent_unique: pw.parent_unique,
        load_pin_cache_body: pw.pin_cache_body,
        load_pin_residency: pw.pin_residency,
        load_pin_new: pw.pin_new,
        load_pin_spent_ms: ns_ms(pw.pin_spent_ns),
        load_pin_body_ms: ns_ms(pw.pin_body_ns),
        load_pin_new_meta_ms: ns_ms(pw.pin_new_meta_ns),
        load_body_tx_reads: pw.body_tx,
        load_parent_tx_reads: pw.parent_tx,
        load_missing_parents: pw.missing,
        load_ready_through,
        cache_bodies,
        cache_plans,
        conf_load_q,
        conf_write_q,
        conf_load_q_cap: super::confirm::LOAD_QUEUE_CAP,
        conf_write_q_cap: super::confirm::WRITE_QUEUE_CAP,
        load_hdr_ms: ns_ms(pw.header_ns),
        load_decode_ms: ns_ms(pw.body_decode_ns),
        load_thin_ms: ns_ms(pw.thin_ns),
        load_parent_pin_ms: ns_ms(pw.parent_pin_ns),
        load_cache_put_ms: ns_ms(pw.cache_put_ns),
        load_edge_same: pw.edge_same_batch,
        load_edge_fk: pw.edge_fk,
        load_edge_cb: pw.edge_coinbase,
        arch_ext_need: arch_res.ext_need,
        arch_sticky_hit: arch_res.sticky_hit,
        arch_head_need: arch_res.head_need,
        arch_head_hit: arch_res.head_hit,
        arch_batch_stamp: arch_res.batch_stamp,
        arch_resolved_stamp: arch_res.resolved_stamp,
        arch_resolve_ns: arch_res.resolve_ns,
        arch_resolve_blocks: arch_res.blocks,
        arch_prep_total_ms: ns_ms(arch_res.prep_total_ns),
        arch_prep_struct_ms: ns_ms(arch_res.prep_struct_ns),
        arch_prep_filter_ms: ns_ms(arch_res.prep_filter_ns),
        arch_prep_assign_ms: ns_ms(arch_res.prep_assign_ns),
        arch_prep_collect_ms: ns_ms(arch_res.prep_collect_ns),
        arch_prep_sticky_ms: ns_ms(arch_res.prep_sticky_ns),
        arch_prep_inflight_ms: ns_ms(arch_res.prep_inflight_ns),
        arch_prep_head_ms: ns_ms(arch_res.prep_head_ns),
        arch_prep_probe_ms: ns_ms(head_res.probe_ns),
        arch_prep_idx_ms: ns_ms(head_res.idx_ns),
        arch_prep_body_txid_ms: ns_ms(head_res.body_ns),
        arch_prep_head_keys: head_res.keys,
        arch_prep_head_cands: head_res.cands,
        arch_prep_body_lookups: head_res.body_lookups,
        arch_prep_stamp_ms: ns_ms(arch_res.prep_stamp_ns),
        arch_prep_finish_ms: ns_ms(arch_res.prep_finish_ns),
        arch_prep_publish_ms: ns_ms(arch_res.prep_publish_ns),
        arch_prep_qwait_ms: ns_ms(arch_res.prep_qwait_ns),
        arch_prep_blocks: arch_res.prep_blocks,
        arch_write_total_ms: ns_ms(arch_res.write_total_ns),
        arch_write_reserve_ms: ns_ms(arch_res.write_reserve_ns),
        arch_write_body_ms: ns_ms(arch_res.write_body_ns),
        arch_write_head_ms: ns_ms(arch_res.write_head_ns),
        arch_write_spend_ms: ns_ms(arch_res.write_spend_ns),
        arch_write_htxs_ms: ns_ms(arch_res.write_htxs_ns),
        arch_write_sticky_ms: ns_ms(arch_res.write_sticky_ns),
        arch_write_dontneed_ms: ns_ms(arch_res.write_dontneed_ns),
        arch_write_flush_ms: ns_ms(arch_res.write_flush_ns),
        arch_write_blocks: arch_res.write_blocks,
        arch_sticky_len,
        arch_sticky_cap,
        contig_next_h,
        contig_parked,
        contig_ready,
        pipe,
        rss_kb: rss.rss_kb,
        rss_anon_kb: rss.anon_kb,
        rss_file_kb: rss.file_kb,
        vm_hwm_kb: rss.hwm_kb,
        rss_locked_kb: rss.locked_kb,
        work,
        owned,
        conf_pipe,
    }
}

fn ns_ms(ns: u64) -> u64 {
    ns / 1_000_000
}

/// Append ` key=value` only when `v != 0` (keeps DEBUG free of ghost columns).
#[inline]
fn append_nz(out: &mut String, key: &str, v: u64) {
    if v != 0 {
        out.push_str(&format!(" {key}={v}"));
    }
}

/// Write-stage wall for this window (structural + class_c + spend annotate + tip GC).
///
/// SH collect is inside class_c wall on the write thread; `sh_ms` is a sub-slice
/// and is **not** double-counted here.
fn write_stage_ms(s: &IbdPerfSample) -> u64 {
    s.structural_ms
        .saturating_add(s.class_c_ms)
        .saturating_add(s.utxo_ms)
        .saturating_add(s.cache_tip_ms)
}

/// Stable INFO line for production grepping (schema-12 pipeline).
pub(crate) fn format_info(s: &IbdPerfSample) -> String {
    let bq_mib = s.bq_bytes / (1024 * 1024);
    let bq_budget_mib = s.bq_budget / (1024 * 1024);
    let write_ms = write_stage_ms(s);
    // Download / queue pressure (what starves tip advance).
    let mut out = format!(
        "ibd: perf inflight={}/{} arch_q={} arch={}/{}MiB bq={} ({}MiB/{}MiB) pending={} arch_ahead={} hole={} peers={}",
        s.inflight,
        s.inflight_cap,
        s.arch_q,
        s.arch_mb,
        s.arch_budget_mb,
        s.bq_count,
        bq_mib,
        bq_budget_mib,
        s.pending,
        s.arch_ahead,
        s.hole,
        s.peers,
    );
    // Three-stage confirm walls (load / script / write) — schema 12 long poles.
    // `connect` is optimistic assemble on the load path (not write structural).
    out.push_str(&format!(
        " | conf blks={} load={}ms script={}ms write={}ms connect={}ms",
        s.phase_blks, s.load_ms, s.script_ms, write_ms, s.connect_ms,
    ));
    append_nz(&mut out, "recon_ms", s.recon_ms);
    append_nz(&mut out, "wire_ms", s.wire_ms);
    append_nz(&mut out, "resolve_ms", s.resolve_ms);

    // Load stage detail: phase walls + pin mix + denserels IO.
    // pin_hit% = (FIFO+same-batch+residency) / (those + cold pin_new after residency).
    let pin_hit_pct = {
        let hits = s.load_pin_cache_body;
        let tot = hits.saturating_add(s.load_pin_new);
        if tot > 0 {
            (100 * hits) / tot
        } else {
            0
        }
    };
    out.push_str(&format!(
        " | load blks={} win={}ms phases hdr={} dec={} put={} thin={} pin={} (fifo={}ms new_io={}ms) pin_hit%={} pin_cache={} pin_res={} pin_new={} body_io={} parent_io={}",
        s.load_blocks,
        s.load_win_ms,
        s.load_hdr_ms,
        s.load_decode_ms,
        s.load_cache_put_ms,
        s.load_thin_ms,
        s.load_parent_pin_ms,
        s.load_pin_body_ms,
        s.load_pin_new_meta_ms,
        pin_hit_pct,
        s.load_pin_cache_body,
        s.load_pin_residency,
        s.load_pin_new,
        s.load_body_tx_reads,
        s.load_parent_tx_reads,
    ));
    if s.load_edge_same > 0 || s.load_edge_fk > 0 || s.load_edge_cb > 0 {
        out.push_str(&format!(
            " edges same={} fk={} cb={}",
            s.load_edge_same, s.load_edge_fk, s.load_edge_cb
        ));
    }
    if s.load_missing_parents > 0 {
        out.push_str(&format!(" miss_p={}", s.load_missing_parents));
    }

    // Write stage detail: structural subs + Class C / SH / spend annotate / tip GC.
    out.push_str(&format!(
        " | write struct={}ms(spent={} create_h={} bip68={}) class_c={}ms sh={}ms spend={}ms tip_gc={}ms",
        s.structural_ms,
        s.structural_spent_ms,
        s.structural_create_h_ms,
        s.structural_bip68_ms,
        s.class_c_ms,
        s.sh_ms,
        s.utxo_ms,
        s.cache_tip_ms,
    ));
    append_nz(&mut out, "strong_ms", s.strong_ms);
    if s.spend_idx > 0 || s.spend_skip > 0 {
        out.push_str(&format!(
            " spend_mix(r={} i={} skip={})",
            s.spend_ranged, s.spend_idx, s.spend_skip
        ));
    }

    let conf_q = super::confirm::format_conf_q(
        s.conf_load_q,
        s.conf_write_q,
        s.conf_load_q_cap,
        s.conf_write_q_cap,
    );
    out.push_str(&format!(
        " | {conf_q} thru={} sh_runs={}",
        s.load_ready_through, s.sh_runs,
    ));

    out.push_str(&format!(
        " | loop {} conf={}ms assign={}ms",
        s.dominant, s.confirm_ms, s.assign_ms,
    ));
    append_nz(&mut out, "getdata", s.assign_issued);
    append_nz(&mut out, "drain_ms", s.drain_ms);
    if s.confirm_reject_stops > 0 {
        out.push_str(&format!(" reject={}", s.confirm_reject_stops));
    }
    if let Some((first, n, elapsed_ms)) = s.live {
        out.push_str(&format!(" | live h={first} n={n} {elapsed_ms}ms"));
    }
    if s.headers_done {
        out.push_str(" headers_done");
    }
    out
}

/// DEBUG detail: µs/blk + pin/edge + archive Class A pipe + contig (no ghost columns).
pub(crate) fn format_debug(s: &IbdPerfSample) -> String {
    let denom = s.phase_blks.max(1);
    let us = |ns: u64| (ns / denom) / 1000;
    let write_ns = s
        .structural_ns
        .saturating_add(s.class_c_ns)
        .saturating_add(s.utxo_apply_ns)
        .saturating_add(s.cache_tip_ns);
    let mut out = format!(
        "ibd: perf_dbg us/blk load={} connect={} script={} write={} struct={} spent={} create_h={} bip68={} class_c={} sh={} spend={}(r={} i={} skip={}) tip_gc={} recon={} wire={}",
        us(s.load_ns),
        us(s.connect_ns),
        us(s.script_ns),
        us(write_ns),
        us(s.structural_ns),
        us(s.structural_spent_ns),
        us(s.structural_create_h_ns),
        us(s.structural_bip68_ns),
        us(s.class_c_ns),
        us(s.sh_ns),
        us(s.utxo_apply_ns),
        s.spend_ranged,
        s.spend_idx,
        s.spend_skip,
        us(s.cache_tip_ns),
        us(s.recon_ns),
        us(s.wire_ns),
    );
    append_nz(&mut out, "resolve_us", us(s.resolve_ns));
    append_nz(&mut out, "strong_us", us(s.strong_ns));
    append_nz(&mut out, "tip_us", us(s.tip_ns));
    // Wire rebuild store cost (live).
    if s.wf_body_store > 0 || s.wf_store_body_ms > 0 {
        out.push_str(&format!(
            " | wire_body store={} store_ms={}",
            s.wf_body_store, s.wf_store_body_ms,
        ));
    }
    // SH: Direct only accrues collect; tip-append fields only if non-zero.
    out.push_str(&format!(" | sh collect={}", s.sh_collect_ms));
    append_nz(&mut out, "filter", s.sh_filter_ms);
    append_nz(&mut out, "sort", s.sh_sort_ms);
    append_nz(&mut out, "seed", s.sh_seed_ms);
    append_nz(&mut out, "body", s.sh_body_ms);
    append_nz(&mut out, "head", s.sh_head_ms);

    let conf_q = super::confirm::format_conf_q(
        s.conf_load_q,
        s.conf_write_q,
        s.conf_load_q_cap,
        s.conf_write_q_cap,
    );
    let bq_mib = s.bq_bytes / (1024 * 1024);
    let bq_budget_mib = s.bq_budget / (1024 * 1024);
    out.push_str(&format!(
        " | bq={} ({}MiB/{}MiB) | {conf_q} | parents thru={} bodies={} plans={} win_ms={} blks={} utxo_p={} creates={} uniq_p={} pin_cache={} pin_res={} pin_new={} body_io={} parent_io={}",
        s.bq_count,
        bq_mib,
        bq_budget_mib,
        s.load_ready_through,
        s.cache_bodies,
        s.cache_plans,
        s.load_win_ms,
        s.load_blocks,
        s.load_utxo_parents,
        s.load_creates,
        s.load_parent_unique,
        s.load_pin_cache_body,
        s.load_pin_residency,
        s.load_pin_new,
        s.load_body_tx_reads,
        s.load_parent_tx_reads,
    ));
    append_nz(&mut out, "miss_p", s.load_missing_parents);
    out.push_str(&format!(
        " phases hdr={} dec={} thin={} pin={} put={} spent={}ms pin_sub body={} new={}",
        s.load_hdr_ms,
        s.load_decode_ms,
        s.load_thin_ms,
        s.load_parent_pin_ms,
        s.load_cache_put_ms,
        s.load_pin_spent_ms,
        s.load_pin_body_ms,
        s.load_pin_new_meta_ms,
    ));
    // Edges: same/fk/cb are live.
    out.push_str(&format!(
        " edges same={} fk={} cb={}",
        s.load_edge_same, s.load_edge_fk, s.load_edge_cb,
    ));
    out.push_str(&format!(" sh_runs={}", s.sh_runs));

    let busy = s.pipe.write_busy_ms();
    let idle = s.pipe.write_idle_ms();
    let total_w = busy.saturating_add(idle).max(1);
    let writer_busy_pct = (100 * busy) / total_w;
    let resolve_us_blk = if s.arch_resolve_blocks > 0 {
        (s.arch_resolve_ns / s.arch_resolve_blocks) / 1000
    } else {
        0
    };
    let sticky_pct = if s.arch_ext_need > 0 {
        (100 * s.arch_sticky_hit) / s.arch_ext_need
    } else {
        0
    };
    // Archive pipe: keep full detail (real long pole on mainnet).
    out.push_str(&format!(
        " | pipe prep_us/blk={} prep_blks={} write_us/blk={} write_blks={} batch_avg={} writer_busy%={} idle_ms={} coalesce_ms={} prep_ms={}",
        s.pipe.prep_us_per_block(),
        s.pipe.prep_blocks,
        s.pipe.write_us_per_block(),
        s.pipe.write_blocks,
        s.pipe.avg_batch(),
        writer_busy_pct,
        idle,
        s.pipe.write_coalesce_ms(),
        s.pipe.prep_ms(),
    ));
    // head_rd = durable parent create_fk **reads** (get_fk_by_txid_batch + subtimers).
    // head_wr = archive **inserts** of this batch's creates (head_insert_many).
    out.push_str(&format!(
        " | arch_prep total={} struct={} assign={} collect={} sticky={} inflight={} head_rd={} (probe={} idx={} body={} keys={} cands={} lookups={}) stamp={} finish={} publish={} qwait={} blks={}",
        s.arch_prep_total_ms,
        s.arch_prep_struct_ms,
        s.arch_prep_assign_ms,
        s.arch_prep_collect_ms,
        s.arch_prep_sticky_ms,
        s.arch_prep_inflight_ms,
        s.arch_prep_head_ms,
        s.arch_prep_probe_ms,
        s.arch_prep_idx_ms,
        s.arch_prep_body_txid_ms,
        s.arch_prep_head_keys,
        s.arch_prep_head_cands,
        s.arch_prep_body_lookups,
        s.arch_prep_stamp_ms,
        s.arch_prep_finish_ms,
        s.arch_prep_publish_ms,
        s.arch_prep_qwait_ms,
        s.arch_prep_blocks,
    ));
    append_nz(&mut out, "prep_filter", s.arch_prep_filter_ms);
    out.push_str(&format!(
        " | arch_write total={} body={} head_wr={} sticky={} dontneed={} flush={} blks={}",
        s.arch_write_total_ms,
        s.arch_write_body_ms,
        s.arch_write_head_ms,
        s.arch_write_sticky_ms,
        s.arch_write_dontneed_ms,
        s.arch_write_flush_ms,
        s.arch_write_blocks,
    ));
    append_nz(&mut out, "write_reserve", s.arch_write_reserve_ms);
    append_nz(&mut out, "write_spend", s.arch_write_spend_ms);
    append_nz(&mut out, "write_htxs", s.arch_write_htxs_ms);
    out.push_str(&format!(
        " | arch_res resolve_us/blk={} ext={} sticky={}/{} ({}%) head={}/{} stamp batch={} res={} sticky_map={}/{}",
        resolve_us_blk,
        s.arch_ext_need,
        s.arch_sticky_hit,
        s.arch_ext_need,
        sticky_pct,
        s.arch_head_hit,
        s.arch_head_need,
        s.arch_batch_stamp,
        s.arch_resolved_stamp,
        s.arch_sticky_len,
        s.arch_sticky_cap,
    ));
    out.push_str(&format!(
        " | contig next_h={} parked={} ready={}",
        s.contig_next_h, s.contig_parked, s.contig_ready,
    ));
    out.push_str(&format!(
        " | loop confirm_blks={} confirm_us/blk={} events={}",
        s.confirm_blocks, s.confirm_us_per_block, s.drain_events,
    ));
    append_nz(&mut out, "reject_stops", s.confirm_reject_stops);
    append_nz(&mut out, "status_scan_ms", s.status_scan_ms);
    out
}

/// Format process RSS + known retain-structure occupancy (leak triage).
///
/// All counts are O(1) lens / brief mutex snaps taken on the 5s tick. Compare
/// `anon=` growth to heap caches and `file=` growth to store mmaps (`tx.head`
/// dual-map during resize). `locked=` is mlock only (usually 0) — **not** a
/// filter on what enters RSS.
pub(crate) fn format_sizes(s: &IbdPerfSample) -> String {
    let w = &s.work;
    let b = &w.body;
    let o = &s.owned;
    let h = &o.head;
    let cp = &s.conf_pipe;
    let head_active = if h.active { 1 } else { 0 };
    let primary_mib = h.primary_body_bytes / (1024 * 1024);
    let shadow_mib = h.shadow_body_bytes / (1024 * 1024);
    let load_wire_mib = cp.load_wire_bytes / (1024 * 1024);
    let write_wire_mib = cp.write_wire_bytes / (1024 * 1024);
    // file% of RSS: how much of process RSS is file-backed (mmap tables + .so).
    let file_pct = if s.rss_kb > 0 {
        (100 * s.rss_file_kb) / s.rss_kb
    } else {
        0
    };
    let bq_mib = s.bq_bytes / (1024 * 1024);
    let bq_budget_mib = s.bq_budget / (1024 * 1024);
    format!(
        "ibd: sizes rss={}MiB anon={}MiB file={}MiB({}%) hwm={}MiB locked={}MiB \
         | work ordered={}/set={} hash_h={} h2h={} hdr_fk={} known_hdr={} inflight={}/peer={} cooldown={} \
         | body known={} pend={} miss={} charged={} rej={} \
         | arch q={}/{}MiB budget={}MiB sticky={}/{} fifo={} contig parked={} ready={} next_h={} \
         | bq n={} {}MiB/{}MiB \
         | residency creates={}/{} outs={}/{} \
         | outfifo creates={} outs={}/{} order={} plans={} \
         | conf loadq={}/{} blks={} wire={}MiB parents={} writeq={}/{} blks={} wire={}MiB parents={} \
           feed ready={} inflight={} \
         | txhead active={} bits={}/{} entry={}B slots={} occ={} body={}MiB \
           shadow bits={} entry={}B slots={} occ={} body={}MiB cursor={}/{} \
         | sh runs={} memtable={} heads={}",
        kb_mib(s.rss_kb),
        kb_mib(s.rss_anon_kb),
        kb_mib(s.rss_file_kb),
        file_pct,
        kb_mib(s.vm_hwm_kb),
        kb_mib(s.rss_locked_kb),
        w.ordered,
        w.ordered_set,
        w.hash_height,
        w.height_to_hash,
        w.header_fks,
        w.known_headers,
        w.inflight,
        w.peer_inflight,
        w.addr_cooldown,
        b.known,
        b.pending,
        b.missing,
        b.archive_charged,
        b.rejected,
        s.arch_q,
        s.arch_mb,
        s.arch_budget_mb,
        o.sticky_len,
        o.sticky_cap,
        o.sticky_fifo,
        s.contig_parked,
        s.contig_ready,
        s.contig_next_h,
        s.bq_count,
        bq_mib,
        bq_budget_mib,
        o.residency_creates,
        o.residency_create_cap,
        o.residency_outs,
        o.residency_out_cap,
        o.out_creates,
        o.out_total,
        o.out_cap,
        o.out_order,
        o.conf_plans,
        cp.load_batches,
        s.conf_load_q_cap,
        cp.load_blocks,
        load_wire_mib,
        cp.load_parents,
        cp.write_batches,
        s.conf_write_q_cap,
        cp.write_blocks,
        write_wire_mib,
        cp.write_parents,
        cp.feed_ready,
        cp.feed_inflight,
        head_active,
        h.primary_bits,
        h.shadow_bits.max(h.primary_bits),
        h.primary_entry_b,
        h.primary_slots,
        h.primary_occupied,
        primary_mib,
        h.shadow_bits,
        h.shadow_entry_b,
        h.shadow_slots,
        h.shadow_occupied,
        shadow_mib,
        h.cursor,
        h.class_a_n,
        o.sh_runs,
        o.sh_memtable,
        o.sh_heads,
    )
}

/// Emit consolidated INFO (+ DEBUG if enabled). Single stderr flush at end.
pub(crate) fn log_sample(s: &IbdPerfSample) {
    info!("{}", format_info(s));
    info!("{}", format_sizes(s));
    if enabled(Level::Debug) {
        debug!("{}", format_debug(s));
    }
    // Surface multi-second write / SH tails that hide in window averages.
    if s.phase_blks > 0 {
        let c_ms = s.class_c_ms / s.phase_blks.max(1);
        let sh_ms = s.sh_ms / s.phase_blks.max(1);
        let recon_ms = s.recon_ms / s.phase_blks.max(1);
        if c_ms >= 1000 || sh_ms >= 1000 || recon_ms >= 5000 {
            rbitcoin_log::warn!(
                "ibd: slow confirm phase ms/blk recon={} script={} class_c={} sh={} (sh_collect={}ms window) store_body={}ms blks={}",
                recon_ms,
                s.script_ms / s.phase_blks.max(1),
                c_ms,
                sh_ms,
                s.sh_collect_ms,
                s.wf_store_body_ms,
                s.phase_blks,
            );
        }
    }
    let _ = std::io::Write::flush(&mut std::io::stderr());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn format_info_has_stable_tokens() {
        let mut s = IbdPerfSample::default();
        s.inflight = 3;
        s.inflight_cap = 256;
        s.arch_q = 10;
        s.bq_count = 7;
        s.bq_bytes = 128 * 1024 * 1024;
        s.bq_budget = 4 * 1024 * 1024 * 1024;
        s.arch_ahead = 224;
        s.hole = 0;
        s.peers = 16;
        s.phase_blks = 32;
        s.recon_ms = 100;
        s.script_ms = 20;
        s.load_ms = 30;
        s.class_c_ms = 40;
        s.utxo_ms = 25;
        s.cache_tip_ms = 5;
        s.dominant = "confirm";
        s.live = Some((100, 32, 1500));
        s.confirm_reject_stops = 2;
        let line = format_info(&s);
        assert!(line.starts_with("ibd: perf "), "{line}");
        assert!(line.contains("inflight=3/256"), "{line}");
        assert!(line.contains("arch_q=10"), "{line}");
        assert!(line.contains("bq=7 (128MiB/4096MiB)"), "{line}");
        assert!(line.contains("arch_ahead=224"), "{line}");
        assert!(!line.contains("lead="), "schema12: no Class A lead= on perf: {line}");
        assert!(!line.contains("arch_hwm"), "{line}");
        assert!(line.contains("conf blks=32"), "{line}");
        assert!(line.contains("script=20ms"), "{line}");
        assert!(line.contains("load=30ms"), "{line}");
        // write = class_c(40)+spend(25)+tip_gc(5) = 70
        assert!(line.contains("write=70ms"), "{line}");
        assert!(line.contains("class_c=40ms"), "{line}");
        assert!(line.contains("spend=25ms"), "{line}");
        assert!(line.contains("struct=0ms"), "{line}");
        assert!(line.contains("recon_ms=100"), "{line}"); // non-zero only
        assert!(!line.contains("prefetch"), "{line}");
        assert!(!line.contains("unpin"), "{line}");
        assert!(line.contains("loop confirm"), "{line}");
        assert!(line.contains("reject=2"), "{line}");
        assert!(line.contains("live h=100 n=32 1500ms"), "{line}");
        s.conf_load_q = 1;
        s.conf_write_q = 2;
        s.conf_load_q_cap = 2;
        s.conf_write_q_cap = 2;
        s.load_ready_through = 200;
        s.load_blocks = 32;
        s.load_pin_cache_body = 8;
        s.load_pin_residency = 3;
        s.load_pin_new = 12;
        s.load_body_tx_reads = 400;
        s.load_parent_tx_reads = 12;
        s.load_win_ms = 40;
        s.load_thin_ms = 5;
        s.load_decode_ms = 15;
        s.load_cache_put_ms = 2;
        s.load_parent_pin_ms = 18;
        s.load_pin_body_ms = 4;
        s.load_pin_new_meta_ms = 14;
        s.sh_runs = 3;
        s.structural_ms = 50;
        s.structural_spent_ms = 30;
        s.structural_create_h_ms = 5;
        s.structural_bip68_ms = 20;
        let line = format_info(&s);
        assert!(line.contains("loadq=1/2 writeq=2/2"), "{line}");
        assert!(line.contains("thru=200"), "{line}");
        assert!(line.contains("pin_cache=8"), "{line}");
        assert!(line.contains("pin_res=3"), "{line}");
        assert!(line.contains("pin_new=12"), "{line}");
        assert!(line.contains("body_io=400 parent_io=12"), "{line}");
        assert!(
            line.contains("struct=50ms(spent=30 create_h=5 bip68=20)"),
            "{line}"
        );
        // write = 50+40+25+5 = 120
        assert!(line.contains("write=120ms"), "{line}");
        // pin_hit% = 8/(8+12) = 40
        assert!(line.contains("pin_hit%=40"), "{line}");
        assert!(line.contains("thin=5"), "{line}");
        assert!(line.contains("put=2"), "{line}");
        assert!(line.contains("new_io=14ms"), "{line}");
        assert!(!line.contains("thin[col="), "{line}");
        assert!(!line.contains("by_fk="), "{line}");
        assert!(!line.contains("pin_cached="), "{line}");
        assert!(line.contains("sh_runs=3"), "{line}");
        assert!(!line.contains("reserved"), "{line}");
        assert!(!line.contains("runway"), "{line}");
    }

    #[test]
    fn format_debug_has_detail_tokens() {
        let mut s = IbdPerfSample::default();
        s.phase_blks = 10;
        s.recon_ns = 10_000_000; // 1ms/blk → 1000 us/blk
        s.utxo_apply_ns = 5_000_000; // 500 us/blk
        s.spend_ranged = 10;
        s.spend_idx = 2;
        s.spend_skip = 0;
        s.wf_body_store = 3;
        s.wf_store_body_ms = 50;
        // (no cache/lock fields — pruned)
        s.conf_load_q = 0;
        s.conf_write_q = 1;
        s.conf_load_q_cap = 2;
        s.conf_write_q_cap = 2;
        s.load_ready_through = 200;
        s.load_blocks = 16;
        s.load_utxo_parents = 100;
        s.load_creates = 50;
        s.load_body_tx_reads = 200;
        s.load_parent_tx_reads = 50;
        s.load_pin_cache_body = 0;
        s.load_pin_residency = 2;
        s.load_pin_new = 38;
        s.load_edge_same = 10;
        s.load_edge_fk = 5;
        s.load_edge_cb = 1;
        s.sh_collect_ms = 12;
        s.pipe.write_blocks = 5;
        s.pipe.write_ns = 5_000_000;
        s.sh_runs = 2;
        s.arch_ext_need = 100;
        s.arch_sticky_hit = 80;
        s.bq_count = 3;
        s.bq_bytes = 64 * 1024 * 1024;
        s.bq_budget = 1024 * 1024 * 1024;
        let line = format_debug(&s);
        assert!(line.starts_with("ibd: perf_dbg "), "{line}");
        assert!(line.contains("us/blk load="), "{line}");
        assert!(line.contains("write="), "{line}");
        assert!(line.contains("spend=500(r=10 i=2 skip=0)"), "{line}");
        assert!(!line.contains("prefetch="), "{line}");
        assert!(!line.contains("wave body="), "{line}");
        assert!(!line.contains("sh seed="), "{line}");
        assert!(!line.contains("thin[col="), "{line}");
        assert!(line.contains("wire_body"), "{line}");
        assert!(line.contains("store_ms=50"), "{line}");
        assert!(line.contains("sh collect=12"), "{line}");
        assert!(line.contains("pin_sub body="), "{line}");
        assert!(line.contains("bq=3 (64MiB/1024MiB)"), "{line}");
        // Depth 0 → `<` (scripts waiting on empty load queue).
        assert!(line.contains("loadq<0/2 writeq=1/2"), "{line}");
        assert!(line.contains("thru=200"), "{line}");
        assert!(line.contains("utxo_p=100"), "{line}");
        assert!(line.contains("creates=50"), "{line}");
        assert!(line.contains("body_io=200 parent_io=50"), "{line}");
        assert!(line.contains("pin_cache=0"), "{line}");
        assert!(line.contains("pin_res=2"), "{line}");
        assert!(line.contains("pin_new=38"), "{line}");
        assert!(!line.contains("pin_cached="), "{line}");
        assert!(line.contains("edges same=10 fk=5 cb=1"), "{line}");
        assert!(line.contains("sh_runs=2"), "{line}");
        assert!(line.contains("arch_res resolve_us/blk="), "{line}");
        assert!(line.contains("arch_prep total="), "{line}");
        assert!(line.contains("probe="), "{line}");
        assert!(line.contains("lookups="), "{line}");
        assert!(line.contains("arch_write total="), "{line}");
        assert!(line.contains("sticky_map="), "{line}");
        assert!(line.contains("sticky=80/100"), "{line}");
        assert!(line.contains("contig next_h="), "{line}");
        assert!(line.contains("pipe "), "{line}");
        assert!(line.contains("loop "), "{line}");
        assert!(!line.contains("runway"), "{line}");
        assert!(!line.contains("connect wave%="), "{line}");
    }

    #[test]
    fn contig_park_stats_snapshot_roundtrip() {
        rbitcoin_query::contig_park_stats::store(42, 7, 3);
        assert_eq!(
            rbitcoin_query::contig_park_stats::snapshot(),
            (42, 7, 3)
        );
    }

    #[test]
    fn format_sizes_has_rss_and_structure_tokens() {
        let mut s = IbdPerfSample::default();
        s.rss_kb = 2 * 1024; // 2 MiB
        s.rss_anon_kb = 1024;
        s.rss_file_kb = 512; // 0 MiB after integer MiB; still shows file=0MiB(25%)
        s.vm_hwm_kb = 3 * 1024;
        s.rss_locked_kb = 0;
        s.arch_q = 12;
        s.arch_mb = 40;
        s.arch_budget_mb = 512;
        s.work.ordered = 100;
        s.work.ordered_set = 90;
        s.work.body.pending = 5;
        s.work.body.archive_charged = 4;
        s.owned.sticky_len = 1000;
        s.owned.sticky_cap = 4_000_000;
        s.owned.residency_creates = 80;
        s.owned.residency_create_cap = 8_000_000;
        s.owned.residency_outs = 900;
        s.owned.residency_out_cap = 16_777_216;
        s.bq_count = 4;
        s.bq_bytes = 32 * 1024 * 1024;
        s.bq_budget = 512 * 1024 * 1024;
        s.owned.out_creates = 50;
        s.owned.out_total = 500;
        s.owned.out_cap = 16_777_216;
        s.owned.head.active = true;
        s.owned.head.primary_bits = 29;
        s.owned.head.shadow_bits = 30;
        s.owned.head.primary_entry_b = 4;
        s.owned.head.shadow_entry_b = 4;
        s.owned.head.primary_slots = 1 << 29;
        s.owned.head.shadow_slots = 1 << 30;
        s.owned.head.primary_body_bytes = (1u64 << 29) * 4;
        s.owned.head.shadow_body_bytes = (1u64 << 30) * 4;
        s.owned.head.cursor = 1_000_000;
        s.owned.head.class_a_n = 2_000_000;
        s.conf_pipe.load_batches = 2;
        s.conf_pipe.load_blocks = 40;
        s.conf_pipe.load_wire_bytes = 12 * 1024 * 1024;
        s.conf_pipe.load_parents = 500;
        s.conf_pipe.write_batches = 1;
        s.conf_pipe.write_blocks = 16;
        s.conf_pipe.write_wire_bytes = 4 * 1024 * 1024;
        s.conf_pipe.feed_ready = 8;
        s.conf_pipe.feed_inflight = 32;
        s.conf_load_q_cap = 5;
        s.conf_write_q_cap = 5;
        s.contig_parked = 3;
        let line = format_sizes(&s);
        assert!(line.starts_with("ibd: sizes "), "{line}");
        assert!(line.contains("rss=2MiB"), "{line}");
        assert!(line.contains("anon=1MiB"), "{line}");
        assert!(line.contains("file=0MiB(25%)"), "{line}"); // 512kB → 0 MiB; pct from kB
        assert!(line.contains("hwm=3MiB"), "{line}");
        assert!(line.contains("locked=0MiB"), "{line}");
        assert!(line.contains("ordered=100/set=90"), "{line}");
        assert!(line.contains("pend=5"), "{line}");
        assert!(line.contains("charged=4"), "{line}");
        assert!(line.contains("arch q=12/40MiB"), "{line}");
        assert!(line.contains("sticky=1000/4000000"), "{line}");
        assert!(line.contains("bq n=4 32MiB/512MiB"), "{line}");
        assert!(line.contains("residency creates=80/8000000 outs=900/16777216"), "{line}");
        assert!(line.contains("outfifo creates=50"), "{line}");
        assert!(line.contains("outs=500/16777216"), "{line}");
        assert!(line.contains("loadq=2/5 blks=40 wire=12MiB parents=500"), "{line}");
        assert!(line.contains("writeq=1/5 blks=16 wire=4MiB"), "{line}");
        assert!(line.contains("feed ready=8 inflight=32"), "{line}");
        assert!(line.contains("txhead active=1"), "{line}");
        assert!(line.contains("bits=29/30"), "{line}");
        assert!(line.contains("cursor=1000000/2000000"), "{line}");
        assert!(line.contains("contig parked=3"), "{line}");
    }

    #[test]
    fn read_proc_rss_returns_nonzero_on_linux() {
        let r = read_proc_rss();
        // Agent VM is Linux with /proc; RSS should be readable for this process.
        assert!(r.rss_kb > 0, "expected VmRSS from /proc/self/status, got {r:?}");
        // Modern kernels expose RssAnon/RssFile on status; at least one side
        // of the split should be non-zero for a running process with heap+.text.
        assert!(
            r.anon_kb > 0 || r.file_kb > 0,
            "expected anon/file split from status or smaps_rollup, got {r:?}"
        );
        // Parts should not wildly exceed total RSS.
        assert!(r.anon_kb <= r.rss_kb.saturating_add(256), "{r:?}");
        assert!(r.file_kb <= r.rss_kb.saturating_add(256), "{r:?}");
        // anon+file ≈ rss (shmem folded into file; allow small accounting skew).
        let sum = r.anon_kb.saturating_add(r.file_kb);
        let skew = sum.abs_diff(r.rss_kb);
        assert!(
            skew <= 1024,
            "anon+file should ≈ rss (±1MiB): sum={sum} skew={skew} {r:?}"
        );
    }

    #[test]
    fn sample_pulls_atomics_and_format_edge_arms() {
        let loop_stats = LoopStats::default();
        loop_stats.confirm_ns.store(2_000_000, Ordering::Relaxed);
        loop_stats.confirm_blocks.store(1, Ordering::Relaxed);
        loop_stats.assign_issued.store(7, Ordering::Relaxed);
        let pipe_stats = ArchivePipelineStats::default();
        pipe_stats.prep_ns.store(1_000_000, Ordering::Relaxed);
        pipe_stats.prep_blocks.store(1, Ordering::Relaxed);

        let work = WorkStructureSizes::default();
        let owned = ProcessOwnedSizes::default();
        let conf_pipe = ConfirmPipelineSizes::default();
        let rss = read_proc_rss();
        let s = sample(
            &loop_stats,
            &pipe_stats,
            4,   // inflight
            256, // cap
            2,   // arch_q
            10,  // arch_mb
            512, // budget
            (512 * 1024 * 1024, 0, 0), // bq budget/bytes/count
            3,   // pending
            0,
            0,
            100, // arch_ahead
            1,   // hole
            8,   // peers
            true, // headers_done
            (50, 10, 0, 0, 0),
            0,
            0,
            1, // sh_runs
            (0, 0),
            work,
            owned,
            conf_pipe,
            rss,
        );
        assert_eq!(s.inflight, 4);
        assert_eq!(s.peers, 8);
        assert!(s.headers_done);
        assert_eq!(s.assign_issued, 7);
        assert_eq!(s.confirm_blocks, 1);
        assert_eq!(s.sh_runs, 1);

        // Edge format arms: spend_mix, miss_p, headers_done, zero pin_hit.
        let mut edge = s.clone();
        edge.spend_idx = 2;
        edge.spend_skip = 1;
        edge.spend_ranged = 3;
        edge.load_missing_parents = 4;
        edge.load_pin_cache_body = 0;
        edge.load_pin_new = 0;
        edge.headers_done = true;
        edge.wire_ms = 9;
        edge.strong_ms = 1;
        edge.resolve_ms = 2;
        edge.drain_ms = 3;
        let info = format_info(&edge);
        assert!(info.contains("spend_mix"), "{info}");
        assert!(info.contains("miss_p=4"), "{info}");
        assert!(info.contains("headers_done"), "{info}");
        assert!(info.contains("wire_ms=9"), "{info}");
        assert!(info.contains("getdata=7"), "{info}");
        assert!(info.contains("pin_hit%=0"), "{info}");

        // log_sample should not panic (INFO path always; DEBUG optional).
        log_sample(&edge);
    }
}
