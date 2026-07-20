//! Consolidated IBD performance sampling and logging.
//!
//! **Cadence:** one sample every ~5s status tick (see `parallel_ibd`).
//!
//! | Level | Message | Contents |
//! |-------|---------|----------|
//! | INFO  | `ibd: perf …` | Pipeline health + coarse confirm (incl. `class_c_ms`, `utxo_ms`) + loop mix + live batch |
//! | DEBUG | `ibd: perf_dbg …` | us/blk (incl. `utxo=`), wave/SH subtimers, caches, pipe, `utxo live/tip/rebuilds`, loop extras |
//!
//! `ibd: progress` stays separate (~1s on tip/arch delta). WARN/ERROR unchanged.
//!
//! Sample **once** per tick and reset all atomics, then format INFO always and
//! DEBUG only when enabled — so DEBUG never sees an empty window after INFO.

use super::archive::{ArchivePipelineSample, ArchivePipelineStats};
use super::LoopStats;
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
    /// Post–Class C light UTXO apply (catch-up only).
    pub utxo_ms: u64,
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

    // Light UTXO snapshot (not phase timers)
    pub utxo_enabled: bool,
    pub utxo_live: u64,
    /// mmap UTXO tip height; `None` if empty/disabled.
    pub utxo_tip: Option<u32>,
    /// Confirm heals this window (apply failed → full rebuild).
    pub utxo_rebuilds: u64,

    // Wave-fill sub
    pub wf_body_ms: u64,
    pub wf_ptx_ms: u64,
    pub wf_pout_ms: u64,
    pub wf_spent_ms: u64,
    pub wf_cb_ms: u64,
    pub wf_tip_note_ms: u64,

    // SH sub
    pub sh_warm_ms: u64,
    pub sh_filter_ms: u64,
    pub sh_collect_ms: u64,
    pub sh_sort_ms: u64,
    pub sh_seed_ms: u64,
    pub sh_body_ms: u64,
    pub sh_head_ms: u64,
    pub sh_index_ms: u64,

    // Caches / connect
    pub ca_hit: u64,
    pub ca_miss: u64,
    pub ca_evict: u64,
    pub tp_hit: u64,
    pub tp_miss: u64,
    pub tp_evict: u64,
    pub tp_note: u64,
    pub tp_retire: u64,
    pub cp_tip: u64,
    pub cp_wave: u64,
    pub cp_class_a: u64,
    pub cp_store: u64,

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
            utxo_enabled: false,
            utxo_live: 0,
            utxo_tip: None,
            utxo_rebuilds: 0,
            wf_body_ms: 0,
            wf_ptx_ms: 0,
            wf_pout_ms: 0,
            wf_spent_ms: 0,
            wf_cb_ms: 0,
            wf_tip_note_ms: 0,
            sh_warm_ms: 0,
            sh_filter_ms: 0,
            sh_collect_ms: 0,
            sh_sort_ms: 0,
            sh_seed_ms: 0,
            sh_body_ms: 0,
            sh_head_ms: 0,
            sh_index_ms: 0,
            ca_hit: 0,
            ca_miss: 0,
            ca_evict: 0,
            tp_hit: 0,
            tp_miss: 0,
            tp_evict: 0,
            tp_note: 0,
            tp_retire: 0,
            cp_tip: 0,
            cp_wave: 0,
            cp_class_a: 0,
            cp_store: 0,
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
    // (enabled, live, tip, rebuilds) from Query::ibd_utxo_perf_snapshot.
    utxo: (bool, u64, Option<u32>, u64),
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
    ) = rbitcoin_consensus::confirm_phase_stats::sample_and_reset();
    let (sh_warm, sh_filter, sh_collect, sh_sort, sh_seed, sh_body, sh_head, sh_index) =
        rbitcoin_query::class_c_phase_stats::sample_sh_sub_and_reset();
    let (wf_body, wf_ptx, wf_pout, wf_spent, wf_cb, wf_tip_note) =
        rbitcoin_query::wave_fill_stats::sample_and_reset();
    let (cah, cam, cae) = rbitcoin_query::class_a_cache_stats::sample_and_reset();
    let (tph, tpm, tpe, tpn, tpr) = rbitcoin_query::tip_prevout_cache_stats::sample_and_reset();
    let (pth, pwh, pca, psm) = rbitcoin_query::connect_prevout_stats::sample_and_reset();
    let pipe = pipe_stats.sample_and_reset();
    let (utxo_enabled, utxo_live, utxo_tip, utxo_rebuilds) = utxo;

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
        utxo_enabled,
        utxo_live,
        utxo_tip,
        utxo_rebuilds,
        wf_body_ms: ns_ms(wf_body),
        wf_ptx_ms: ns_ms(wf_ptx),
        wf_pout_ms: ns_ms(wf_pout),
        wf_spent_ms: ns_ms(wf_spent),
        wf_cb_ms: ns_ms(wf_cb),
        wf_tip_note_ms: ns_ms(wf_tip_note),
        sh_warm_ms: ns_ms(sh_warm),
        sh_filter_ms: ns_ms(sh_filter),
        sh_collect_ms: ns_ms(sh_collect),
        sh_sort_ms: ns_ms(sh_sort),
        sh_seed_ms: ns_ms(sh_seed),
        sh_body_ms: ns_ms(sh_body),
        sh_head_ms: ns_ms(sh_head),
        sh_index_ms: ns_ms(sh_index),
        ca_hit: cah,
        ca_miss: cam,
        ca_evict: cae,
        tp_hit: tph,
        tp_miss: tpm,
        tp_evict: tpe,
        tp_note: tpn,
        tp_retire: tpr,
        cp_tip: pth,
        cp_wave: pwh,
        cp_class_a: pca,
        cp_store: psm,
        pipe,
    }
}

/// Stable INFO line for production grepping.
pub(crate) fn format_info(s: &IbdPerfSample) -> String {
    let mut out = format!(
        "ibd: perf inflight={}/{} arch_q={} arch={}/{}MiB pending={} known_arch={} ordered={} ahead={} hole={} headers_done={} peers={}",
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
        s.headers_done,
        s.peers,
    );
    // Coarse confirm: recon split + connect/script + class_c + SH/tip + UTXO apply.
    out.push_str(&format!(
        " | blks={} recon_ms={}(p={} w={} wire={}) connect_ms={} script_ms={} class_c_ms={} strong_ms={} sh_ms={} tip_ms={} utxo_ms={}",
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
    ));
    out.push_str(&format!(
        " | dominant={} confirm_ms={} assign_ms={} getdata={} drain_ms={}",
        s.dominant, s.confirm_ms, s.assign_ms, s.assign_issued, s.drain_ms,
    ));
    if s.confirm_reject_stops > 0 {
        out.push_str(&format!(" reject_stops={}", s.confirm_reject_stops));
    }
    if let Some((first, n, elapsed_ms)) = s.live {
        out.push_str(&format!(
            " | live first={first} batch={n} elapsed_ms={elapsed_ms}"
        ));
    }
    out
}

/// DEBUG detail line (former multi-line phase/cache/pipe dump).
pub(crate) fn format_debug(s: &IbdPerfSample) -> String {
    let denom = s.phase_blks.max(1);
    let us = |ns: u64| (ns / denom) / 1000;
    let mut out = format!(
        "ibd: perf_dbg us/blk recon={} prefetch={} wave={} wire={} connect={} script={} class_c={} strong={} sh={} tip={} utxo={}",
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
    );
    out.push_str(&format!(
        " | wave body={} ptx={} pout={} spent={} cb={} tip_note={}",
        s.wf_body_ms,
        s.wf_ptx_ms,
        s.wf_pout_ms,
        s.wf_spent_ms,
        s.wf_cb_ms,
        s.wf_tip_note_ms,
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
    let ca_tot = s.ca_hit + s.ca_miss;
    let ca_pct = if ca_tot > 0 {
        (100 * s.ca_hit) / ca_tot
    } else {
        0
    };
    let tp_tot = s.tp_hit + s.tp_miss;
    let tp_pct = if tp_tot > 0 {
        (100 * s.tp_hit) / tp_tot
    } else {
        0
    };
    let cp_tot = s.cp_tip + s.cp_wave + s.cp_class_a + s.cp_store;
    out.push_str(&format!(
        " | ca hit={} miss={} evict={} hit%={} tip_po hit={} miss={} evict={} note={} retire={} hit%={} connect tip%={} wave%={} class_a%={} store%={}",
        s.ca_hit,
        s.ca_miss,
        s.ca_evict,
        ca_pct,
        s.tp_hit,
        s.tp_miss,
        s.tp_evict,
        s.tp_note,
        s.tp_retire,
        tp_pct,
        if cp_tot > 0 {
            (100 * s.cp_tip) / cp_tot
        } else {
            0
        },
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
    if s.utxo_enabled {
        out.push_str(&format!(
            " | utxo live={} tip={} rebuilds={}",
            s.utxo_live,
            s.utxo_tip
                .map(|t| t.to_string())
                .unwrap_or_else(|| "none".into()),
            s.utxo_rebuilds,
        ));
    } else {
        out.push_str(&format!(" | utxo off rebuilds={}", s.utxo_rebuilds));
    }
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
        s.phase_blks = 32;
        s.recon_ms = 100;
        s.prefetch_ms = 10;
        s.wave_ms = 80;
        s.wire_ms = 10;
        s.class_c_ms = 40;
        s.utxo_ms = 25;
        s.dominant = "confirm";
        s.live = Some((100, 32, 1500));
        s.confirm_reject_stops = 2;
        let line = format_info(&s);
        assert!(line.starts_with("ibd: perf "), "{line}");
        assert!(line.contains("inflight=3/256"), "{line}");
        assert!(line.contains("arch_q=10"), "{line}");
        assert!(line.contains("blks=32"), "{line}");
        assert!(line.contains("recon_ms=100(p=10 w=80 wire=10)"), "{line}");
        assert!(line.contains("class_c_ms=40"), "{line}");
        assert!(line.contains("utxo_ms=25"), "{line}");
        assert!(line.contains("dominant=confirm"), "{line}");
        assert!(line.contains("reject_stops=2"), "{line}");
        assert!(line.contains("live first=100 batch=32 elapsed_ms=1500"), "{line}");
        // No separate legacy prefixes.
        assert!(!line.contains("confirm_phases"));
        assert!(!line.contains("wave_fill_phases"));
        assert!(!line.contains("spent_local"), "{line}");
    }

    #[test]
    fn format_debug_has_detail_tokens() {
        let mut s = IbdPerfSample::default();
        s.phase_blks = 10;
        s.recon_ns = 10_000_000; // 1ms/blk → 1000 us/blk
        s.utxo_apply_ns = 5_000_000; // 500 us/blk
        s.wf_spent_ms = 50;
        s.ca_hit = 90;
        s.ca_miss = 10;
        s.pipe.write_blocks = 5;
        s.pipe.write_ns = 5_000_000;
        s.utxo_enabled = true;
        s.utxo_live = 12_345;
        s.utxo_tip = Some(100);
        s.utxo_rebuilds = 0;
        let line = format_debug(&s);
        assert!(line.starts_with("ibd: perf_dbg "), "{line}");
        assert!(line.contains("us/blk recon="), "{line}");
        assert!(line.contains("utxo=500"), "{line}"); // us/blk
        assert!(!line.contains("spent_local"), "{line}");
        assert!(line.contains("wave body="), "{line}");
        assert!(line.contains("spent=50"), "{line}"); // wave spent filter subtimer
        assert!(line.contains("ca hit=90 miss=10"), "{line}");
        assert!(line.contains("hit%=90"), "{line}");
        assert!(line.contains("pipe "), "{line}");
        assert!(line.contains("utxo live=12345 tip=100 rebuilds=0"), "{line}");
        assert!(line.contains("loop "), "{line}");
    }
}
