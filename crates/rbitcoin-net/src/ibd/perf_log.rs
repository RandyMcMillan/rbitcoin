//! Consolidated IBD performance sampling and logging.
//!
//! **Cadence:** one centralized ~5s status tick (see `ibd` main loop) emits both
//! `ibd: progress` and `ibd: perf` together.
//!
//! | Level | Message | Contents |
//! |-------|---------|----------|
//! | INFO  | `ibd: progress …` | Tip/arch rates over the **last 5s**, confirm pipeline queue depths, horizon, **1h tip ETA** (bold on TTY) |
//! | INFO  | `ibd: perf …` | Download queue, archive pressure, confirm cost, load phases, conf_q, loop mix |
//! | DEBUG | `ibd: perf_dbg …` | µs/blk phases, wave/SH subs, caches, pipe |
//!
//! Sample **once** per tick and reset all atomics, then format INFO always and
//! DEBUG only when enabled — so DEBUG never sees an empty window after INFO.

use super::archive::{ArchivePipelineSample, ArchivePipelineStats};
use super::status::LoopStats;
use rbitcoin_log::{debug, enabled, info, Level};

/// One 5s window of IBD counters (post sample-and-reset).
#[derive(Clone, Debug)]
pub(crate) struct IbdPerfSample {
    // Pipeline health (not from atomics).
    pub inflight: usize,
    pub inflight_cap: usize,
    pub arch_q: usize,
    pub arch_mb: usize,
    pub arch_budget_mb: usize,
    pub pending: usize,
    pub known_arch: usize,
    pub ordered: usize,
    pub ahead: u32,
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
    pub prefetch_ms: u64,
    pub wave_ms: u64,
    pub wire_ms: u64,
    pub connect_ms: u64,
    pub script_ms: u64,
    pub class_c_ms: u64,
    pub strong_ms: u64,
    pub sh_ms: u64,
    pub tip_ms: u64,
    /// Post–Class C durable spend annotate wall (logged as `spend=` ms).
    pub utxo_ms: u64,
    /// Spend annotate path mix: body-range / idx / skipped (null create_fk).
    pub spend_ranged: u64,
    pub spend_idx: u64,
    pub spend_skip: u64,
    /// Formerly unaccounted confirm overhead (ms totals).
    pub resolve_ms: u64,
    pub load_ms: u64,
    pub unpin_ms: u64,
    pub cache_tip_ms: u64,
    // raw ns for us/blk
    pub recon_ns: u64,
    pub prefetch_ns: u64,
    pub wave_ns: u64,
    pub wire_ns: u64,
    pub connect_ns: u64,
    pub script_ns: u64,
    pub class_c_ns: u64,
    pub strong_ns: u64,
    pub sh_ns: u64,
    pub tip_ns: u64,
    pub utxo_apply_ns: u64,
    pub resolve_ns: u64,
    pub load_ns: u64,
    pub unpin_ns: u64,
    pub cache_tip_ns: u64,

    /// Confirm-load mlocked ranges / unique page RAM.
    pub mlock_ranges: usize,
    pub mlock_bytes: u64,
    /// On-disk scripthash sorted runs waiting for tip bulk.
    pub sh_runs: usize,

    // Wave-fill sub
    pub wf_body_ms: u64,
    pub wf_ptx_ms: u64,
    pub wf_pout_ms: u64,
    pub wf_spent_ms: u64,
    pub wf_cb_ms: u64,
    /// Wave bodies moved from parent cache vs re-decoded from store.
    pub wf_body_cache: u64,
    pub wf_body_store: u64,
    /// Thin edges moved from cache stash vs rebuilt from inputs.
    pub wf_thin_cache: u64,
    pub wf_thin_rebuild: u64,
    /// Store body decode wall (ms) during wire rebuild.
    pub wf_store_body_ms: u64,
    /// ConfirmParentCache mutex wait (ms).
    pub wf_cache_lock_ms: u64,

    // SH sub
    pub sh_warm_ms: u64,
    pub sh_filter_ms: u64,
    pub sh_collect_ms: u64,
    pub sh_sort_ms: u64,
    pub sh_seed_ms: u64,
    pub sh_body_ms: u64,
    pub sh_head_ms: u64,
    pub sh_index_ms: u64,

    // Connect prevout resolve mix
    pub cp_wave: u64,
    pub cp_class_a: u64,
    pub cp_store: u64,

    // Parent cache / confirm-load window counters + live snapshot
    /// Wall ms in `load_confirm_parents` this window (sampler).
    pub load_win_ms: u64,
    pub load_blocks: u64,
    pub load_utxo_parents: u64,
    pub load_creates: u64,
    pub load_already_ready: u64,
    pub load_parent_unique: u64,
    /// Of uniq_p: by_fk re-pin / cache body / store decode.
    pub load_pin_already_cached: u64,
    pub load_pin_cache_body: u64,
    pub load_pin_new: u64,
    pub load_pin_cover_miss_no_fk: u64,
    pub load_pin_cover_miss_partial: u64,
    /// Spent-filter wall on pin path (ms).
    pub load_pin_spent_ms: u64,
    /// Mlock wall on pin path (ms).
    pub load_pin_mlock_ms: u64,

    /// Phase-1 body Class A reads this window.
    pub load_body_tx_reads: u64,
    /// Phase-2 external parent pins this window.
    pub load_parent_tx_reads: u64,
    pub load_missing_parents: u64,
    /// Contiguous ready watermark height.
    pub load_ready_through: u32,
    /// Parent-cache snapshot (bodies still held / plans / depth).
    pub cache_parents: usize,
    pub cache_bodies: usize,
    pub cache_plans: usize,
    /// Confirm pipeline queue depths (load→scripts, scripts→write).
    pub conf_load_q: usize,
    pub conf_write_q: usize,
    pub conf_load_q_cap: usize,
    pub conf_write_q_cap: usize,
    /// Runway internal phase ms (window sum).
    pub load_hdr_ms: u64,
    pub load_body_mlock_ms: u64,
    pub load_decode_ms: u64,
    pub load_thin_ms: u64,
    /// Thin sub-phases (ms window sum).
    pub load_thin_collect_ms: u64,
    pub load_thin_cache_ms: u64,
    pub load_thin_head_ms: u64,
    pub load_thin_edge_ms: u64,
    pub load_parent_pin_ms: u64,
    pub load_cache_put_ms: u64,
    pub load_head_lookups: u64,
    pub load_head_hits: u64,
    pub load_mlock_sys: u64,
    pub load_mlock_skip: u64,
    pub load_edge_same: u64,
    pub load_edge_cache: u64,
    /// Stamped create_fk, parent outside batch (not a RAM cache hit).
    pub load_edge_fk: u64,
    pub load_edge_head: u64,
    pub load_edge_cb: u64,

    // Archive create_fk resolve (window)
    pub arch_ext_need: u64,
    pub arch_sticky_hit: u64,
    pub arch_head_need: u64,
    pub arch_head_hit: u64,
    pub arch_batch_stamp: u64,
    pub arch_resolved_stamp: u64,
    pub arch_resolve_ns: u64,
    pub arch_resolve_blocks: u64,
    /// Live sticky map size (not window-reset).
    pub arch_sticky_len: usize,
    pub arch_sticky_cap: usize,

    /// ContigPark live snapshot (writer; not window-reset).
    pub contig_next_h: u32,
    pub contig_parked: usize,
    pub contig_ready: usize,

    // Pipe
    pub pipe: ArchivePipelineSample,
}

impl Default for IbdPerfSample {
    fn default() -> Self {
        Self {
            inflight: 0,
            inflight_cap: 0,
            arch_q: 0,
            arch_mb: 0,
            arch_budget_mb: 0,
            pending: 0,
            known_arch: 0,
            ordered: 0,
            ahead: 0,
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
            prefetch_ms: 0,
            wave_ms: 0,
            wire_ms: 0,
            connect_ms: 0,
            script_ms: 0,
            class_c_ms: 0,
            strong_ms: 0,
            sh_ms: 0,
            tip_ms: 0,
            utxo_ms: 0,
            spend_ranged: 0,
            spend_idx: 0,
            spend_skip: 0,
            resolve_ms: 0,
            load_ms: 0,
            unpin_ms: 0,
            cache_tip_ms: 0,
            recon_ns: 0,
            prefetch_ns: 0,
            wave_ns: 0,
            wire_ns: 0,
            connect_ns: 0,
            script_ns: 0,
            class_c_ns: 0,
            strong_ns: 0,
            sh_ns: 0,
            tip_ns: 0,
            utxo_apply_ns: 0,
            resolve_ns: 0,
            load_ns: 0,
            unpin_ns: 0,
            cache_tip_ns: 0,
            mlock_ranges: 0,
            mlock_bytes: 0,
            sh_runs: 0,
            wf_body_ms: 0,
            wf_ptx_ms: 0,
            wf_pout_ms: 0,
            wf_spent_ms: 0,
            wf_cb_ms: 0,
            wf_body_cache: 0,
            wf_body_store: 0,
            wf_thin_cache: 0,
            wf_thin_rebuild: 0,
            wf_store_body_ms: 0,
            wf_cache_lock_ms: 0,
            sh_warm_ms: 0,
            sh_filter_ms: 0,
            sh_collect_ms: 0,
            sh_sort_ms: 0,
            sh_seed_ms: 0,
            sh_body_ms: 0,
            sh_head_ms: 0,
            sh_index_ms: 0,
            cp_wave: 0,
            cp_class_a: 0,
            cp_store: 0,
            load_win_ms: 0,
            load_blocks: 0,
            load_utxo_parents: 0,
            load_creates: 0,
            load_already_ready: 0,
            load_parent_unique: 0,
            load_pin_already_cached: 0,
            load_pin_cache_body: 0,
            load_pin_new: 0,
            load_pin_cover_miss_no_fk: 0,
            load_pin_cover_miss_partial: 0,
            load_pin_spent_ms: 0,
            load_pin_mlock_ms: 0,

            load_body_tx_reads: 0,
            load_parent_tx_reads: 0,
            load_missing_parents: 0,
            load_ready_through: 0,
            cache_parents: 0,
            cache_bodies: 0,
            cache_plans: 0,
            conf_load_q: 0,
            conf_write_q: 0,
            conf_load_q_cap: super::confirm::LOAD_QUEUE_CAP,
            conf_write_q_cap: super::confirm::WRITE_QUEUE_CAP,
            load_hdr_ms: 0,
            load_body_mlock_ms: 0,
            load_decode_ms: 0,
            load_thin_ms: 0,
            load_thin_collect_ms: 0,
            load_thin_cache_ms: 0,
            load_thin_head_ms: 0,
            load_thin_edge_ms: 0,
            load_parent_pin_ms: 0,
            load_cache_put_ms: 0,
            load_head_lookups: 0,
            load_head_hits: 0,
            load_mlock_sys: 0,
            load_mlock_skip: 0,
            load_edge_same: 0,
            load_edge_cache: 0,
            load_edge_fk: 0,
            load_edge_head: 0,
            load_edge_cb: 0,
            arch_ext_need: 0,
            arch_sticky_hit: 0,
            arch_head_need: 0,
            arch_head_hit: 0,
            arch_batch_stamp: 0,
            arch_resolved_stamp: 0,
            arch_resolve_ns: 0,
            arch_resolve_blocks: 0,
            arch_sticky_len: 0,
            arch_sticky_cap: 0,
            contig_next_h: 0,
            contig_parked: 0,
            contig_ready: 0,
            pipe: ArchivePipelineSample::default(),
        }
    }
}

fn ns_ms(ns: u64) -> u64 {
    ns / 1_000_000
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
    pending: usize,
    known_arch: usize,
    ordered: usize,
    ahead: u32,
    hole: usize,
    peers: usize,
    headers_done: bool,
    // (ready_through, ahead, parents, bodies, plans).
    load: (u32, u32, usize, usize, usize),
    conf_load_q: usize,
    conf_write_q: usize,
    mlock_ranges: usize,
    mlock_bytes: u64,
    sh_runs: usize,
    // Live archive sticky (len, cap).
    arch_sticky: (usize, usize),
) -> IbdPerfSample {
    let hot = loop_stats.sample_and_reset();
    let (
        recon_ns,
        prefetch_ns,
        wave_fill_ns,
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
        unpin_ns,
        cache_tip_ns,
        spend_ranged,
        spend_idx,
        spend_skip,
    ) = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    let _ = rbitcoin_query::ibd_utxo_stats::sample_probe_flush_and_reset();
    let (sh_warm, sh_filter, sh_collect, sh_sort, sh_seed, sh_body, sh_head, sh_index) =
        rbitcoin_query::class_c_phase_stats::sample_sh_sub_and_reset();
    let (wf_body, wf_ptx, wf_pout, wf_spent, wf_cb) =
        rbitcoin_query::wave_fill_stats::sample_and_reset();
    let (wf_body_cache, wf_body_store, wf_thin_cache, wf_thin_rebuild) =
        rbitcoin_query::wave_fill_stats::sample_counts_and_reset();
    let (wf_store_body_ns, wf_cache_lock_ns) =
        rbitcoin_query::wave_fill_stats::sample_io_and_reset();
    let (pwh, pca, psm) = rbitcoin_query::connect_prevout_stats::sample_and_reset();
    let pw = rbitcoin_query::confirm_load_stats::sample_and_reset();
    let arch_res = rbitcoin_query::archive_resolve_stats::sample_and_reset();
    let pipe = pipe_stats.sample_and_reset();
    let (contig_next_h, contig_parked, contig_ready) =
        rbitcoin_query::contig_park_stats::snapshot();
    let (load_ready_through, _cache_ahead, cache_parents, cache_bodies, cache_plans) = load;
    let (arch_sticky_len, arch_sticky_cap) = arch_sticky;

    IbdPerfSample {
        inflight,
        inflight_cap,
        arch_q,
        arch_mb,
        arch_budget_mb,
        pending,
        known_arch,
        ordered,
        ahead,
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
        prefetch_ms: ns_ms(prefetch_ns),
        wave_ms: ns_ms(wave_fill_ns),
        wire_ms: ns_ms(wire_ns),
        connect_ms: ns_ms(connect_ns),
        script_ms: ns_ms(script_ns),
        class_c_ms: ns_ms(class_c_ns),
        strong_ms: ns_ms(strong_ns),
        sh_ms: ns_ms(sh_ns),
        tip_ms: ns_ms(tip_ns),
        utxo_ms: ns_ms(utxo_apply_ns),
        spend_ranged,
        spend_idx,
        spend_skip,
        resolve_ms: ns_ms(resolve_ns),
        load_ms: ns_ms(load_ns),
        unpin_ms: ns_ms(unpin_ns),
        cache_tip_ms: ns_ms(cache_tip_ns),
        recon_ns,
        prefetch_ns,
        wave_ns: wave_fill_ns,
        wire_ns,
        connect_ns,
        script_ns,
        class_c_ns,
        strong_ns,
        sh_ns,
        tip_ns,
        utxo_apply_ns,
        resolve_ns,
        load_ns,
        unpin_ns,
        cache_tip_ns,
        mlock_ranges,
        mlock_bytes,
        sh_runs,
        wf_body_ms: ns_ms(wf_body),
        wf_ptx_ms: ns_ms(wf_ptx),
        wf_pout_ms: ns_ms(wf_pout),
        wf_spent_ms: ns_ms(wf_spent),
        wf_cb_ms: ns_ms(wf_cb),
        wf_body_cache,
        wf_body_store,
        wf_thin_cache,
        wf_thin_rebuild,
        wf_store_body_ms: ns_ms(wf_store_body_ns),
        wf_cache_lock_ms: ns_ms(wf_cache_lock_ns),
        sh_warm_ms: ns_ms(sh_warm),
        sh_filter_ms: ns_ms(sh_filter),
        sh_collect_ms: ns_ms(sh_collect),
        sh_sort_ms: ns_ms(sh_sort),
        sh_seed_ms: ns_ms(sh_seed),
        sh_body_ms: ns_ms(sh_body),
        sh_head_ms: ns_ms(sh_head),
        sh_index_ms: ns_ms(sh_index),
        cp_wave: pwh,
        cp_class_a: pca,
        cp_store: psm,
        load_win_ms: ns_ms(pw.ns),
        load_blocks: pw.blocks,
        load_utxo_parents: pw.utxo_parents,
        load_creates: pw.creates,
        load_already_ready: pw.already_ready,
        load_parent_unique: pw.parent_unique,
        load_pin_already_cached: pw.pin_already_cached,
        load_pin_cache_body: pw.pin_cache_body,
        load_pin_new: pw.pin_new,
        load_pin_cover_miss_no_fk: pw.pin_cover_miss_no_fk,
        load_pin_cover_miss_partial: pw.pin_cover_miss_partial,
        load_pin_spent_ms: ns_ms(pw.pin_spent_ns),
        load_pin_mlock_ms: ns_ms(pw.pin_mlock_ns),

        load_body_tx_reads: pw.body_tx,
        load_parent_tx_reads: pw.parent_tx,
        load_missing_parents: pw.missing,
        load_ready_through,
        cache_parents,
        cache_bodies,
        cache_plans,
        conf_load_q,
        conf_write_q,
        conf_load_q_cap: super::confirm::LOAD_QUEUE_CAP,
        conf_write_q_cap: super::confirm::WRITE_QUEUE_CAP,
        load_hdr_ms: ns_ms(pw.header_ns),
        load_body_mlock_ms: ns_ms(pw.body_mlock_ns),
        load_decode_ms: ns_ms(pw.body_decode_ns),
        load_thin_ms: ns_ms(pw.thin_ns),
        load_thin_collect_ms: ns_ms(pw.thin_collect_ns),
        load_thin_cache_ms: ns_ms(pw.thin_cache_ns),
        load_thin_head_ms: ns_ms(pw.thin_head_ns),
        load_thin_edge_ms: ns_ms(pw.thin_edge_ns),
        load_parent_pin_ms: ns_ms(pw.parent_pin_ns),
        load_cache_put_ms: ns_ms(pw.cache_put_ns),
        load_head_lookups: pw.head_lookups,
        load_head_hits: pw.head_hits,
        load_mlock_sys: pw.mlock_syscalls,
        load_mlock_skip: pw.mlock_skipped,
        load_edge_same: pw.edge_same_batch,
        load_edge_cache: pw.edge_cache,
        load_edge_fk: pw.edge_fk,
        load_edge_head: pw.edge_head,
        load_edge_cb: pw.edge_coinbase,
        arch_ext_need: arch_res.ext_need,
        arch_sticky_hit: arch_res.sticky_hit,
        arch_head_need: arch_res.head_need,
        arch_head_hit: arch_res.head_hit,
        arch_batch_stamp: arch_res.batch_stamp,
        arch_resolved_stamp: arch_res.resolved_stamp,
        arch_resolve_ns: arch_res.resolve_ns,
        arch_resolve_blocks: arch_res.blocks,
        arch_sticky_len,
        arch_sticky_cap,
        contig_next_h,
        contig_parked,
        contig_ready,
        pipe,
    }
}

/// Stable INFO line for production grepping.
pub(crate) fn format_info(s: &IbdPerfSample) -> String {
    // Download / archive pressure (what blocks the tip).
    let mut out = format!(
        "ibd: perf inflight={}/{} arch_q={} arch={}/{}MiB pending={} known_arch={} ordered={} lead={} hole={} peers={}",
        s.inflight,
        s.inflight_cap,
        s.arch_q,
        s.arch_mb,
        s.arch_budget_mb,
        s.pending,
        s.known_arch,
        s.ordered,
        s.ahead,
        s.hole,
        s.peers,
    );
    // Confirm cost this window (ms totals + block count).
    out.push_str(&format!(
        " | conf blks={} recon={}ms(p={} w={} wire={}) connect={}ms script={}ms class_c={}ms strong={}ms sh={}ms tip={}ms spend={}ms(r={} i={} skip={}) | ovh resolve={}ms load={}ms unpin={}ms tip_gc={}ms",
        s.phase_blks,
        s.recon_ms,
        s.prefetch_ms,
        s.wave_ms,
        s.wire_ms,
        s.connect_ms,
        s.script_ms,
        s.class_c_ms,
        s.strong_ms,
        s.sh_ms,
        s.tip_ms,
        s.utxo_ms,
        s.spend_ranged,
        s.spend_idx,
        s.spend_skip,
        s.resolve_ms,
        s.load_ms,
        s.unpin_ms,
        s.cache_tip_ms,
    ));
    out.push_str(&format!(
        " | loop {} conf={}ms assign={}ms getdata={} drain={}ms",
        s.dominant, s.confirm_ms, s.assign_ms, s.assign_issued, s.drain_ms,
    ));
    if s.confirm_reject_stops > 0 {
        out.push_str(&format!(" reject={}", s.confirm_reject_stops));
    }
    if let Some((first, n, elapsed_ms)) = s.live {
        out.push_str(&format!(" | live h={first} n={n} {elapsed_ms}ms"));
    }
    // Pin reuse rate (unique parents): already-covered + body-in-cache vs store pin.
    // (Old formula mixed per-edge thin hits with unique parents — inflated / meaningless.)
    let pin_hit_pct = {
        let hits = s
            .load_pin_already_cached
            .saturating_add(s.load_pin_cache_body);
        let tot = hits.saturating_add(s.load_pin_new);
        if tot > 0 {
            (100 * hits) / tot
        } else {
            0
        }
    };
    let mlock_mb = s.mlock_bytes / (1024 * 1024);
    let conf_q = super::confirm::format_conf_q(
        s.conf_load_q,
        s.conf_write_q,
        s.conf_load_q_cap,
        s.conf_write_q_cap,
    );
    out.push_str(&format!(
        " | {conf_q} | parents thru={} by_fk={} bodies={} plans={} blks={} body_io={} parent_io={} pin_cached={} pin_cache={} pin_new={} pin_hit%={} {}ms (hdr={} mlock={} dec={} thin={}[col={} run={} head={} edge={}] pin={} put={}) spent={}ms mlock_pin={}ms miss_nf={} miss_part={} head={}/{} mlock_sys={}/{} mlock={mlock_mb}MiB ranges={} sh_runs={}",
        s.load_ready_through,
        s.cache_parents,
        s.cache_bodies,
        s.cache_plans,
        s.load_blocks,
        s.load_body_tx_reads,
        s.load_parent_tx_reads,
        s.load_pin_already_cached,
        s.load_pin_cache_body,
        s.load_pin_new,
        pin_hit_pct,
        s.load_win_ms,
        s.load_hdr_ms,
        s.load_body_mlock_ms,
        s.load_decode_ms,
        s.load_thin_ms,
        s.load_thin_collect_ms,
        s.load_thin_cache_ms,
        s.load_thin_head_ms,
        s.load_thin_edge_ms,
        s.load_parent_pin_ms,
        s.load_cache_put_ms,
        s.load_pin_spent_ms,
        s.load_pin_mlock_ms,
        s.load_pin_cover_miss_no_fk,
        s.load_pin_cover_miss_partial,
        s.load_head_hits,
        s.load_head_lookups,
        s.load_mlock_sys,
        s.load_mlock_skip,
        s.mlock_ranges,
        s.sh_runs,
    ));
    if s.load_missing_parents > 0 {
        out.push_str(&format!(" miss_p={}", s.load_missing_parents));
    }
    if s.headers_done {
        out.push_str(" headers_done");
    }
    out
}

/// DEBUG detail line (former multi-line phase/cache/pipe dump).
pub(crate) fn format_debug(s: &IbdPerfSample) -> String {
    let denom = s.phase_blks.max(1);
    let us = |ns: u64| (ns / denom) / 1000;
    let mut out = format!(
        "ibd: perf_dbg us/blk recon={} prefetch={} wave={} wire={} connect={} script={} class_c={} strong={} sh={} tip={} spend={}(r={} i={} skip={}) | ovh resolve={} load={} unpin={} tip_gc={}",
        us(s.recon_ns),
        us(s.prefetch_ns),
        us(s.wave_ns),
        us(s.wire_ns),
        us(s.connect_ns),
        us(s.script_ns),
        us(s.class_c_ns),
        us(s.strong_ns),
        us(s.sh_ns),
        us(s.tip_ns),
        us(s.utxo_apply_ns),
        s.spend_ranged,
        s.spend_idx,
        s.spend_skip,
        us(s.resolve_ns),
        us(s.load_ns),
        us(s.unpin_ns),
        us(s.cache_tip_ns),
    );
    out.push_str(&format!(
        " | wave body={} ptx={} pout={} spent={} cb={} cache={} store={} thin={} rebuild={} store_ms={} lock_ms={}",
        s.wf_body_ms,
        s.wf_ptx_ms,
        s.wf_pout_ms,
        s.wf_spent_ms,
        s.wf_cb_ms,
        s.wf_body_cache,
        s.wf_body_store,
        s.wf_thin_cache,
        s.wf_thin_rebuild,
        s.wf_store_body_ms,
        s.wf_cache_lock_ms,
    ));
    out.push_str(&format!(
        " | sh warm={} filter={} collect={} sort={} seed={} body={} head={} index={}",
        s.sh_warm_ms,
        s.sh_filter_ms,
        s.sh_collect_ms,
        s.sh_sort_ms,
        s.sh_seed_ms,
        s.sh_body_ms,
        s.sh_head_ms,
        s.sh_index_ms,
    ));
    let cp_tot = s.cp_wave + s.cp_class_a + s.cp_store;
    let mlock_mb = s.mlock_bytes / (1024 * 1024);
    let conf_q = super::confirm::format_conf_q(
        s.conf_load_q,
        s.conf_write_q,
        s.conf_load_q_cap,
        s.conf_write_q_cap,
    );
    out.push_str(&format!(
        " | {conf_q} | parents thru={} by_fk={} bodies={} plans={} win_ms={} blks={} utxo_p={} creates={} skip={} uniq_p={} pin_cached={} pin_cache={} pin_new={} body_io={} parent_io={} miss_p={} phases_ms hdr={} mlock={} dec={} thin={}[col={} run={} head={} edge={}] pin={} put={} spent={}ms mlock_pin={}ms miss_nf={} miss_part={} head={}/{} mlock_sys={}/{} edges same={} cache={} fk={} head={} cb={} mlock={mlock_mb}MiB ranges={} sh_runs={} | connect wave%={} parent%={} store%={}",
        s.load_ready_through,
        s.cache_parents,
        s.cache_bodies,
        s.cache_plans,
        s.load_win_ms,
        s.load_blocks,
        s.load_utxo_parents,
        s.load_creates,
        s.load_already_ready,
        s.load_parent_unique,
        s.load_pin_already_cached,
        s.load_pin_cache_body,
        s.load_pin_new,
        s.load_body_tx_reads,
        s.load_parent_tx_reads,
        s.load_missing_parents,
        s.load_hdr_ms,
        s.load_body_mlock_ms,
        s.load_decode_ms,
        s.load_thin_ms,
        s.load_thin_collect_ms,
        s.load_thin_cache_ms,
        s.load_thin_head_ms,
        s.load_thin_edge_ms,
        s.load_parent_pin_ms,
        s.load_cache_put_ms,
        s.load_pin_spent_ms,
        s.load_pin_mlock_ms,
        s.load_pin_cover_miss_no_fk,
        s.load_pin_cover_miss_partial,
        s.load_head_hits,
        s.load_head_lookups,
        s.load_mlock_sys,
        s.load_mlock_skip,
        s.load_edge_same,
        s.load_edge_cache,
        s.load_edge_fk,
        s.load_edge_head,
        s.load_edge_cb,
        s.mlock_ranges,
        s.sh_runs,
        if cp_tot > 0 {
            (100 * s.cp_wave) / cp_tot
        } else {
            0
        },
        if cp_tot > 0 {
            (100 * s.cp_class_a) / cp_tot
        } else {
            0
        },
        if cp_tot > 0 {
            (100 * s.cp_store) / cp_tot
        } else {
            0
        },
    ));
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
    out.push_str(&format!(
        " | pipe prep_us/blk={} prep_blks={} write_us/blk={} write_blks={} batch_avg={} writer_busy%={} idle_ms={} coalesce_ms={} prep_ms={} | arch_res resolve_us/blk={} ext={} sticky={}/{} ({}%) head={}/{} stamp batch={} res={} sticky_map={}/{} | contig next_h={} parked={} ready={}",
        s.pipe.prep_us_per_block(),
        s.pipe.prep_blocks,
        s.pipe.write_us_per_block(),
        s.pipe.write_blocks,
        s.pipe.avg_batch(),
        writer_busy_pct,
        idle,
        s.pipe.write_coalesce_ms(),
        s.pipe.prep_ms(),
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
        s.contig_next_h,
        s.contig_parked,
        s.contig_ready,
    ));
    out.push_str(&format!(
        " | loop confirm_blks={} confirm_us/blk={} reject_stops={} events={} status_scan_ms={}",
        s.confirm_blocks,
        s.confirm_us_per_block,
        s.confirm_reject_stops,
        s.drain_events,
        s.status_scan_ms,
    ));
    out
}

/// Emit consolidated INFO (+ DEBUG if enabled). Single stderr flush at end.
pub(crate) fn log_sample(s: &IbdPerfSample) {
    info!("{}", format_info(s));
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
                "ibd: slow confirm phase ms/blk recon={} script={} class_c={} sh={} (sh_collect={}ms window) store_body={}ms cache_lock={}ms blks={}",
                recon_ms,
                s.script_ms / s.phase_blks.max(1),
                c_ms,
                sh_ms,
                s.sh_collect_ms,
                s.wf_store_body_ms,
                s.wf_cache_lock_ms,
                s.phase_blks,
            );
        }
    }
    let _ = std::io::Write::flush(&mut std::io::stderr());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_info_has_stable_tokens() {
        let mut s = IbdPerfSample::default();
        s.inflight = 3;
        s.inflight_cap = 256;
        s.arch_q = 10;
        s.ahead = 224;
        s.hole = 0;
        s.peers = 16;
        s.phase_blks = 32;
        s.recon_ms = 100;
        s.script_ms = 20;
        s.class_c_ms = 40;
        s.utxo_ms = 25;
        s.dominant = "confirm";
        s.live = Some((100, 32, 1500));
        s.confirm_reject_stops = 2;
        let line = format_info(&s);
        assert!(line.starts_with("ibd: perf "), "{line}");
        assert!(line.contains("inflight=3/256"), "{line}");
        assert!(line.contains("arch_q=10"), "{line}");
        assert!(line.contains("lead=224"), "{line}");
        assert!(line.contains("conf blks=32"), "{line}");
        assert!(line.contains("recon=100ms"), "{line}");
        assert!(line.contains("class_c=40ms"), "{line}");
        assert!(line.contains("spend=25ms(r=0 i=0 skip=0)"), "{line}");
        assert!(line.contains("loop confirm"), "{line}");
        assert!(line.contains("reject=2"), "{line}");
        assert!(line.contains("live h=100 n=32 1500ms"), "{line}");
        s.conf_load_q = 1;
        s.conf_write_q = 2;
        s.conf_load_q_cap = 2;
        s.conf_write_q_cap = 2;
        s.load_ready_through = 200;
        s.cache_parents = 12;
        s.cache_bodies = 48;
        s.cache_plans = 80;
        s.load_blocks = 32;
        s.load_body_tx_reads = 400;
        s.load_parent_tx_reads = 120;
        s.load_parent_unique = 20;
        s.load_pin_already_cached = 5;
        s.load_pin_cache_body = 3;
        s.load_pin_new = 12;
        s.load_win_ms = 40;
        s.mlock_bytes = 32 * 1024 * 1024;
        s.mlock_ranges = 12;
        s.sh_runs = 3;
        let line = format_info(&s);
        assert!(line.contains("conf_q load=1/2 write=2/2"), "{line}");
        assert!(line.contains("thru=200"), "{line}");
        assert!(line.contains("by_fk=12 bodies=48 plans=80"), "{line}");
        assert!(line.contains("body_io=400 parent_io=120"), "{line}");
        assert!(line.contains("pin_cached=5 pin_cache=3 pin_new=12"), "{line}");
        // pin_hit% = (5+3)/(5+3+12) = 40
        assert!(line.contains("pin_hit%=40"), "{line}");
        assert!(!line.contains("cache%="), "{line}");
        assert!(line.contains("mlock=32MiB ranges=12 sh_runs=3"), "{line}");
        assert!(!line.contains("reserved"), "{line}");
        assert!(!line.contains("confirm_phases"), "{line}");
        assert!(!line.contains("mat="), "{line}");
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
        s.wf_spent_ms = 50;
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
        s.load_pin_already_cached = 12;
        s.load_pin_new = 38;
        s.pipe.write_blocks = 5;
        s.pipe.write_ns = 5_000_000;
        s.mlock_bytes = 16 * 1024 * 1024;
        s.mlock_ranges = 4;
        s.sh_runs = 2;
        let line = format_debug(&s);
        assert!(line.starts_with("ibd: perf_dbg "), "{line}");
        assert!(line.contains("us/blk recon="), "{line}");
        assert!(line.contains("spend=500(r=10 i=2 skip=0)"), "{line}"); // us/blk wall
        assert!(line.contains("wave body="), "{line}");
        assert!(line.contains("spent=50"), "{line}");
        assert!(line.contains("cache="), "{line}");
        assert!(line.contains("store="), "{line}");
        assert!(line.contains("thin="), "{line}");
        assert!(line.contains("rebuild="), "{line}");
        // Depth 0 → `<` (scripts waiting on empty load queue).
        assert!(line.contains("conf_q load<0/2 write=1/2"), "{line}");
        assert!(line.contains("thru=200"), "{line}");
        assert!(line.contains("utxo_p=100"), "{line}");
        assert!(line.contains("creates=50"), "{line}");
        assert!(line.contains("body_io=200 parent_io=50"), "{line}");
        assert!(line.contains("pin_cached=12 pin_cache=0 pin_new=38"), "{line}");
        assert!(line.contains("mlock=16MiB ranges=4 sh_runs=2"), "{line}");
        assert!(line.contains("arch_res resolve_us/blk="), "{line}");
        assert!(line.contains("sticky_map="), "{line}");
        assert!(line.contains("store_ms="), "{line}");
        assert!(line.contains("lock_ms="), "{line}");
        assert!(!line.contains("majflt="), "{line}");
        assert!(line.contains("contig next_h="), "{line}");
        assert!(!line.contains("reserved"), "{line}");
        assert!(line.contains("pipe "), "{line}");
        assert!(line.contains("loop "), "{line}");
        assert!(!line.contains("runway"), "{line}");
    }

    #[test]
    fn contig_park_stats_snapshot_roundtrip() {
        rbitcoin_query::contig_park_stats::store(42, 7, 3);
        assert_eq!(
            rbitcoin_query::contig_park_stats::snapshot(),
            (42, 7, 3)
        );
    }
}
