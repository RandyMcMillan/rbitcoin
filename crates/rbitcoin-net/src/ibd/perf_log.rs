//! Consolidated IBD performance sampling and logging.
//!
//! **Cadence:** one sample every ~5s status tick (see `ibd`).
//!
//! | Level | Message | Contents |
//! |-------|---------|----------|
//! | INFO  | `ibd: perf …` | Download queue, archive pressure, confirm cost, prewarm lead/IO, loop mix |
//! | DEBUG | `ibd: perf_dbg …` | µs/blk phases, wave/SH subs, caches, pipe |
//!
//! `ibd: progress` (~1s on tip/arch delta) is the operator glance line: tip rate,
//! archive lead, tip hole, peers, prewarm lead, mlock RAM. WARN/ERROR unchanged.
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
    pub prewarm_wait_ms: u64,
    pub unpin_ms: u64,
    pub runway_tip_ms: u64,
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
    pub prewarm_wait_ns: u64,
    pub unpin_ns: u64,
    pub runway_tip_ns: u64,

    /// Confirm prewarm mlocked ranges / unique page RAM.
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

    // Parent prewarm (window counters + live snapshot)
    /// Wall ms spent prewarming this window.
    pub pw_ms: u64,
    pub pw_blocks: u64,
    pub pw_utxo_parents: u64,
    pub pw_creates: u64,
    pub pw_already_ready: u64,
    pub pw_parent_unique: u64,
    pub pw_cache_hits: u64,
    /// Phase-1 body Class A reads this window.
    pub pw_body_tx_reads: u64,
    /// Phase-2 external parent pins this window.
    pub pw_parent_tx_reads: u64,
    pub pw_missing_parents: u64,
    /// Contiguous ready watermark height.
    pub pw_ready_through: u32,
    /// `ready_through - tip` (blocks warmer is ahead of confirm tip).
    pub pw_ahead: u32,
    pub pw_parents: usize,
    /// Full bodies cached for the runway.
    pub pw_bodies: usize,
    pub pw_plans: usize,
    pub pw_depth: u32,
    /// Prewarm internal phase ms (window sum).
    pub pw_hdr_ms: u64,
    pub pw_body_mlock_ms: u64,
    pub pw_decode_ms: u64,
    pub pw_thin_ms: u64,
    pub pw_parent_pin_ms: u64,
    pub pw_cache_put_ms: u64,
    pub pw_head_lookups: u64,
    pub pw_head_hits: u64,
    pub pw_mlock_sys: u64,
    pub pw_mlock_skip: u64,
    pub pw_edge_same: u64,
    pub pw_edge_runway: u64,
    pub pw_edge_head: u64,
    pub pw_edge_cb: u64,

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
            prewarm_wait_ms: 0,
            unpin_ms: 0,
            runway_tip_ms: 0,
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
            prewarm_wait_ns: 0,
            unpin_ns: 0,
            runway_tip_ns: 0,
            mlock_ranges: 0,
            mlock_bytes: 0,
            sh_runs: 0,
            wf_body_ms: 0,
            wf_ptx_ms: 0,
            wf_pout_ms: 0,
            wf_spent_ms: 0,
            wf_cb_ms: 0,
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
            pw_ms: 0,
            pw_blocks: 0,
            pw_utxo_parents: 0,
            pw_creates: 0,
            pw_already_ready: 0,
            pw_parent_unique: 0,
            pw_cache_hits: 0,
            pw_body_tx_reads: 0,
            pw_parent_tx_reads: 0,
            pw_missing_parents: 0,
            pw_ready_through: 0,
            pw_ahead: 0,
            pw_parents: 0,
            pw_bodies: 0,
            pw_plans: 0,
            pw_depth: 0,
            pw_hdr_ms: 0,
            pw_body_mlock_ms: 0,
            pw_decode_ms: 0,
            pw_thin_ms: 0,
            pw_parent_pin_ms: 0,
            pw_cache_put_ms: 0,
            pw_head_lookups: 0,
            pw_head_hits: 0,
            pw_mlock_sys: 0,
            pw_mlock_skip: 0,
            pw_edge_same: 0,
            pw_edge_runway: 0,
            pw_edge_head: 0,
            pw_edge_cb: 0,
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
    // (ready_through, ahead, parents, bodies, plans, depth).
    prewarm: (u32, u32, usize, usize, usize, u32),
    mlock_ranges: usize,
    mlock_bytes: u64,
    sh_runs: usize,
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
        prewarm_wait_ns,
        unpin_ns,
        runway_tip_ns,
        spend_ranged,
        spend_idx,
        spend_skip,
    ) = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    let _ = rbitcoin_query::ibd_utxo_stats::sample_probe_flush_and_reset();
    let (sh_warm, sh_filter, sh_collect, sh_sort, sh_seed, sh_body, sh_head, sh_index) =
        rbitcoin_query::class_c_phase_stats::sample_sh_sub_and_reset();
    let (wf_body, wf_ptx, wf_pout, wf_spent, wf_cb) =
        rbitcoin_query::wave_fill_stats::sample_and_reset();
    let (pwh, pca, psm) = rbitcoin_query::connect_prevout_stats::sample_and_reset();
    let pw = rbitcoin_query::parent_prewarm_stats::sample_and_reset();
    let pipe = pipe_stats.sample_and_reset();
    let (pw_ready_through, pw_ahead, pw_parents, pw_bodies, pw_plans, pw_depth) = prewarm;

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
        prewarm_wait_ms: ns_ms(prewarm_wait_ns),
        unpin_ms: ns_ms(unpin_ns),
        runway_tip_ms: ns_ms(runway_tip_ns),
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
        prewarm_wait_ns,
        unpin_ns,
        runway_tip_ns,
        mlock_ranges,
        mlock_bytes,
        sh_runs,
        wf_body_ms: ns_ms(wf_body),
        wf_ptx_ms: ns_ms(wf_ptx),
        wf_pout_ms: ns_ms(wf_pout),
        wf_spent_ms: ns_ms(wf_spent),
        wf_cb_ms: ns_ms(wf_cb),
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
        pw_ms: ns_ms(pw.ns),
        pw_blocks: pw.blocks,
        pw_utxo_parents: pw.utxo_parents,
        pw_creates: pw.creates,
        pw_already_ready: pw.already_ready,
        pw_parent_unique: pw.parent_unique,
        pw_cache_hits: pw.cache_hits,
        pw_body_tx_reads: pw.body_tx,
        pw_parent_tx_reads: pw.parent_tx,
        pw_missing_parents: pw.missing,
        pw_ready_through,
        pw_ahead,
        pw_parents,
        pw_bodies,
        pw_plans,
        pw_depth,
        pw_hdr_ms: ns_ms(pw.header_ns),
        pw_body_mlock_ms: ns_ms(pw.body_mlock_ns),
        pw_decode_ms: ns_ms(pw.body_decode_ns),
        pw_thin_ms: ns_ms(pw.thin_ns),
        pw_parent_pin_ms: ns_ms(pw.parent_pin_ns),
        pw_cache_put_ms: ns_ms(pw.cache_put_ns),
        pw_head_lookups: pw.head_lookups,
        pw_head_hits: pw.head_hits,
        pw_mlock_sys: pw.mlock_syscalls,
        pw_mlock_skip: pw.mlock_skipped,
        pw_edge_same: pw.edge_same_batch,
        pw_edge_runway: pw.edge_runway,
        pw_edge_head: pw.edge_head,
        pw_edge_cb: pw.edge_coinbase,
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
        " | conf blks={} recon={}ms(p={} w={} wire={}) connect={}ms script={}ms class_c={}ms strong={}ms sh={}ms tip={}ms spend={}ms(r={} i={} skip={}) | ovh resolve={}ms pw_wait={}ms unpin={}ms tip_gc={}ms",
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
        s.prewarm_wait_ms,
        s.unpin_ms,
        s.runway_tip_ms,
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
    // Prewarm: how far ahead of tip + Class A IO mix this window.
    let cache_pct = {
        let hits = s.pw_cache_hits;
        let loads = s.pw_parent_unique;
        let tot = hits.saturating_add(loads);
        if tot > 0 {
            (100 * hits) / tot
        } else {
            0
        }
    };
    let mlock_mb = s.mlock_bytes / (1024 * 1024);
    out.push_str(&format!(
        " | prewarm +{} thru={} by_txid={} bodies={} plans={}/{} blks={} body_io={} parent_io={} cache%={} {}ms (hdr={} mlock={} dec={} thin={} pin={} put={}) head={}/{} mlock_sys={}/{} mlock={mlock_mb}MiB ranges={} sh_runs={}",
        s.pw_ahead,
        s.pw_ready_through,
        s.pw_parents,
        s.pw_bodies,
        s.pw_plans,
        s.pw_depth,
        s.pw_blocks,
        s.pw_body_tx_reads,
        s.pw_parent_tx_reads,
        cache_pct,
        s.pw_ms,
        s.pw_hdr_ms,
        s.pw_body_mlock_ms,
        s.pw_decode_ms,
        s.pw_thin_ms,
        s.pw_parent_pin_ms,
        s.pw_cache_put_ms,
        s.pw_head_hits,
        s.pw_head_lookups,
        s.pw_mlock_sys,
        s.pw_mlock_skip,
        s.mlock_ranges,
        s.sh_runs,
    ));
    if s.pw_missing_parents > 0 {
        out.push_str(&format!(" miss_p={}", s.pw_missing_parents));
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
        "ibd: perf_dbg us/blk recon={} prefetch={} wave={} wire={} connect={} script={} class_c={} strong={} sh={} tip={} spend={}(r={} i={} skip={}) | ovh resolve={} pw_wait={} unpin={} tip_gc={}",
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
        us(s.prewarm_wait_ns),
        us(s.unpin_ns),
        us(s.runway_tip_ns),
    );
    out.push_str(&format!(
        " | wave body={} ptx={} pout={} spent={} cb={}",
        s.wf_body_ms,
        s.wf_ptx_ms,
        s.wf_pout_ms,
        s.wf_spent_ms,
        s.wf_cb_ms,
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
    out.push_str(&format!(
        " | prewarm +{} thru={} by_txid={} bodies={} plans={}/{} win_ms={} blks={} utxo_p={} creates={} skip={} uniq_p={} cache_hit={} body_io={} parent_io={} miss_p={} phases_ms hdr={} mlock={} dec={} thin={} pin={} put={} head={}/{} mlock_sys={}/{} edges same={} runway={} head={} cb={} mlock={mlock_mb}MiB ranges={} sh_runs={} | connect wave%={} parent%={} store%={}",
        s.pw_ahead,
        s.pw_ready_through,
        s.pw_parents,
        s.pw_bodies,
        s.pw_plans,
        s.pw_depth,
        s.pw_ms,
        s.pw_blocks,
        s.pw_utxo_parents,
        s.pw_creates,
        s.pw_already_ready,
        s.pw_parent_unique,
        s.pw_cache_hits,
        s.pw_body_tx_reads,
        s.pw_parent_tx_reads,
        s.pw_missing_parents,
        s.pw_hdr_ms,
        s.pw_body_mlock_ms,
        s.pw_decode_ms,
        s.pw_thin_ms,
        s.pw_parent_pin_ms,
        s.pw_cache_put_ms,
        s.pw_head_hits,
        s.pw_head_lookups,
        s.pw_mlock_sys,
        s.pw_mlock_skip,
        s.pw_edge_same,
        s.pw_edge_runway,
        s.pw_edge_head,
        s.pw_edge_cb,
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
        s.pw_ahead = 64;
        s.pw_ready_through = 200;
        s.pw_parents = 12;
        s.pw_bodies = 48;
        s.pw_plans = 80;
        s.pw_depth = 256;
        s.pw_blocks = 32;
        s.pw_body_tx_reads = 400;
        s.pw_parent_tx_reads = 120;
        s.pw_cache_hits = 80;
        s.pw_parent_unique = 20;
        s.pw_ms = 40;
        s.mlock_bytes = 32 * 1024 * 1024;
        s.mlock_ranges = 12;
        s.sh_runs = 3;
        let line = format_info(&s);
        assert!(line.contains("prewarm +64 thru=200"), "{line}");
        assert!(line.contains("by_txid=12 bodies=48 plans=80/256"), "{line}");
        assert!(line.contains("body_io=400 parent_io=120"), "{line}");
        assert!(line.contains("mlock=32MiB ranges=12 sh_runs=3"), "{line}");
        assert!(!line.contains("reserved"), "{line}");
        assert!(!line.contains("confirm_phases"), "{line}");
        assert!(!line.contains("pause_fetch"), "{line}");
        assert!(!line.contains("mat="), "{line}");
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
        s.pw_ahead = 64;
        s.pw_ready_through = 200;
        s.pw_blocks = 16;
        s.pw_utxo_parents = 100;
        s.pw_creates = 50;
        s.pw_body_tx_reads = 200;
        s.pw_parent_tx_reads = 50;
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
        assert!(line.contains("prewarm +64 thru=200"), "{line}");
        assert!(line.contains("utxo_p=100"), "{line}");
        assert!(line.contains("creates=50"), "{line}");
        assert!(line.contains("body_io=200 parent_io=50"), "{line}");
        assert!(line.contains("mlock=16MiB ranges=4 sh_runs=2"), "{line}");
        assert!(!line.contains("reserved"), "{line}");
        assert!(!line.contains("pause_fetch"), "{line}");
        assert!(line.contains("pipe "), "{line}");
        assert!(line.contains("loop "), "{line}");
    }
}
