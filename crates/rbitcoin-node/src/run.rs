use crate::config::NodeConfig;
use crate::error::NodeError;
use bitcoin::consensus::Encodable;
use rbitcoin_electrum::{run_electrum, ElectrumConfig, TipNotify};
use rbitcoin_esplora::{run_esplora, EsploraConfig};
use rbitcoin_log::{debug, enabled, info, warn, Level};
use rbitcoin_net::{default_port, AddrMan, IbdConfig, MempoolHub, P2PNode, TipEvent};
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;
use rbitcoin_wire_cache::WireRing;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Notify};

/// Running node state (store open; optional P2P).
pub struct NodeHandle {
    pub config: NodeConfig,
    pub query: Query,
    pub wire: WireRing,
    /// Durable cluster mempool (opened in `run_p2p` and attached to `ChainHub`).
    /// Smoke-only `run_node` leaves this `None`.
    pub mempool: Option<std::sync::Arc<MempoolHub>>,
}

impl std::fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle")
            .field("config", &self.config)
            .field("network", &self.config.network)
            .field(
                "mempool_gen",
                &self.mempool.as_ref().map(|m| m.generation()),
            )
            .finish()
    }
}

impl NodeHandle {
    pub fn network_name(&self) -> &'static str {
        self.config.network.as_str()
    }

    pub fn shutdown(self) -> Result<(), NodeError> {
        self.query.flush()?;
        if let Some(mp) = &self.mempool {
            mp.flush()
                .map_err(|e| NodeError::Config(format!("mempool flush: {e}")))?;
        }
        Ok(())
    }
}

/// Cooperative shutdown flag shared across the process lifetime.
#[derive(Debug)]
pub struct Shutdown {
    /// Polled by IBD / long loops for cooperative cancel.
    pub flag: Arc<AtomicBool>,
    notify: Notify,
}

impl Shutdown {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            flag: Arc::new(AtomicBool::new(false)),
            notify: Notify::new(),
        })
    }

    pub fn request(&self) {
        if !self.flag.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    pub fn requested(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Completes when shutdown has been requested.
    pub async fn cancelled(&self) {
        if self.requested() {
            return;
        }
        self.notify.notified().await;
        while !self.requested() {
            self.notify.notified().await;
        }
    }
}

/// Install SIGTERM / SIGINT (and Ctrl+C) handlers that trip `shutdown`.
fn spawn_signal_handler(shutdown: Arc<Shutdown>) {
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    warn!("signal: failed to install SIGTERM handler: {e}");
                    return;
                }
            };
            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    warn!("signal: failed to install SIGINT handler: {e}");
                    return;
                }
            };
            tokio::select! {
                _ = sigterm.recv() => info!("signal: received SIGTERM"),
                _ = sigint.recv() => info!("signal: received SIGINT"),
            }
        }
        #[cfg(not(unix))]
        {
            if let Err(e) = tokio::signal::ctrl_c().await {
                warn!("signal: ctrl_c error: {e}");
                return;
            }
            info!("signal: received Ctrl+C");
        }
        shutdown.request();
    });
}

/// Start the node: ensure datadir, open store, prepare tip wire ring.
pub fn run_node(config: NodeConfig) -> Result<NodeHandle, NodeError> {
    config.ensure_datadir()?;
    let query = Query::open_or_create(config.store_path())?;
    let wire_dir = config.datadir.join("wire");
    let wire = WireRing::with_dir(config.wire_depth_blocks, wire_dir)
        .map_err(|e| NodeError::Config(format!("wire ring: {e}")))?;
    // Mempool is opened in `run_p2p` after ChainHub has `Arc<Query>`.
    Ok(NodeHandle {
        config,
        query,
        wire,
        mempool: None,
    })
}

/// Long-running P2P (+ optional Electrum): seed resolve, catch-up, persistent follow, progress logs.
///
/// Cleanly exits on **SIGTERM** / **SIGINT** (`kill <pid>` or Ctrl+C): flushes the store
/// and aborts peer tasks.
pub async fn run_p2p(config: NodeConfig) -> Result<(), NodeError> {
    let handle = run_node(config.clone())?;
    let params = config.chain_params()?;
    let milestone = config.milestone();
    if milestone.height > 0 {
        info!(
            "ibd: milestone height={} (script/sig checks skipped at/below; prevouts always)",
            milestone.height
        );
    }
    // Index mode selection on restart:
    // - **Tip-ready** (durable SH covers Class A tip, no residual runs): stay Tip —
    //   tip-follow only a few blocks behind must not Class A recollect / rematerialize.
    // - Otherwise Direct IBD: archive tx.head, confirm spends, SH runs → bulk at tip.
    let sh_tip_ready = handle.query.sh_is_tip_ready();
    if sh_tip_ready {
        let _ = handle.query.sync_sh_seal_from_include_hwm();
        handle.query.enter_tip_index_mode();
        info!(
            "node: durable scripthash covers tip (include_hwm/SEAL) — resume IndexMode::Tip \
             (skip Direct Class A recollect; short catch-up uses durable SH write-through)"
        );
    } else {
        handle
            .query
            .enter_direct_index_mode()
            .map_err(|e| NodeError::Config(format!("index direct mode: {e}")))?;
        info!(
            "ibd: IndexMode::Direct (archive tx.head; confirm spend batch; SH runs merge-only; bulk SH at tip)"
        );
    }
    let listen = config
        .p2p_listen
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], default_port(config.network))));

    let start_tip = handle.query.tip_height().map(|h| h.0).unwrap_or(0);
    let run_started = Instant::now();
    let head_access = rbitcoin_store::head_table_access_from_env();
    info!(
        "rbitcoin-node starting version={} network={} datadir={} tip={start_tip} \
         tx_head_access={head_access:?} io={}",
        env!("CARGO_PKG_VERSION"),
        config.network.as_str(),
        config.datadir.display(),
        std::env::var("RBITCOIN_IO").unwrap_or_else(|_| "default".into()),
    );

    let query = handle.query;
    let mut node = P2PNode::start(listen, query, params.clone(), milestone)
        .await
        .map_err(|e| NodeError::Config(format!("p2p start: {e}")))?;

    let mempool = MempoolHub::open_with_weight(
        config.mempool_path(),
        node.hub.query.clone(),
        config.mempool_max_weight,
    )
    .map_err(|e| NodeError::Config(e))?;
    node.hub
        .attach_mempool(mempool.clone())
        .map_err(|_| NodeError::Config("mempool already attached".into()))?;
    info!(
        "mempool: open {} gen={} live={} max_weight={} (relay off until tip mode)",
        config.mempool_path().display(),
        mempool.generation(),
        mempool.live_count(),
        config.mempool_max_weight
    );

    info!(
        "rbitcoin-node listening on {} ({})",
        node.local_addr,
        config.network.as_str()
    );

    let shutdown = Shutdown::new();
    spawn_signal_handler(shutdown.clone());

    // Persisted peer book (discovered addrs + PeerFlags) under datadir.
    let peers_path = config.datadir.join("peers");
    let mut addrman = match AddrMan::load(&peers_path) {
        Ok(am) => {
            if !am.is_empty() {
                info!(
                    "peers: loaded {} address(es) with flags from {}",
                    am.len(),
                    peers_path.display()
                );
            }
            am
        }
        Err(e) => {
            warn!(
                "peers: load {}: {e} — starting empty book",
                peers_path.display()
            );
            AddrMan::new()
        }
    };
    for c in &config.connect {
        addrman.add(*c);
    }
    if should_resolve_default_seeds(&config) {
        info!(
            "ibd: resolving DNS/fixed seeds for {}…",
            config.network.as_str()
        );
        let n_before = addrman.len();
        addrman.inject(rbitcoin_net::resolve_all_seeds(config.network));
        info!(
            "ibd: seeds resolved (+{} new, book={})",
            addrman.len().saturating_sub(n_before),
            addrman.len()
        );
    } else if config.signet_challenge.is_some() && config.connect.is_empty() && addrman.is_empty() {
        warn!("custom signet has no peers; use --connect ADDR or reuse a datadir with known peers");
    }
    // Shared with IBD so learned addrs/flags flush back on IBD exit.
    let shared_peers = std::sync::Arc::new(std::sync::Mutex::new(addrman.clone()));

    let max_out = config.max_outbound.max(1) as usize;
    // Seed **candidates** (pool) vs live **target_peers** (max_out). Pool is
    // larger so connect failures still leave enough live download peers.
    let candidate_n = max_out.saturating_mul(2).clamp(16, 48);
    let targets = if !config.connect.is_empty() {
        config.connect.clone()
    } else {
        addrman.take_outbound(max_out)
    };

    // Catch-up: concurrent multi-peer download window (libbitcoin-class).
    let ibd_targets = if !config.connect.is_empty() {
        config.connect.clone()
    } else {
        // Prefer a wide ranked sample (flag-aware, IPv4-first).
        addrman.take_outbound(candidate_n)
    };
    // True only after IBD reports true catch-up (or no peers to dial).
    // Mid-chain peer death must not enter tip mode (materialize durable indexes).
    let mut catch_up_complete = ibd_targets.is_empty();
    if !ibd_targets.is_empty() && !shutdown.requested() {
        let target_peers = max_out.clamp(8, 32);
        let ibd_cfg = IbdConfig {
            // Concurrent getdata cap 1024; per-peer in-transit 16 (archive may lead tip).
            window: rbitcoin_net::DEFAULT_IBD_WINDOW,
            per_peer: rbitcoin_net::DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER,
            target_peers,
            // 5s caused reassign storms (clearing 200+ inflight before peers
            // could deliver mid-chain blocks). Default 30s is enough.
            stall: std::time::Duration::from_secs(30),
            peers: Some(std::sync::Arc::clone(&shared_peers)),
            ..IbdConfig::default()
        };
        info!(
            "ibd: catch-up candidates={} target_peers={} (window={}, per_peer={})…",
            ibd_targets.len(),
            ibd_cfg.target_peers,
            ibd_cfg.window,
            ibd_cfg.per_peer
        );
        // Cooperative cancel only: IBD polls `shutdown.flag` and exits its own
        // teardown path. Do **not** `select!`+drop the IBD future on SIGINT —
        // that used to drop a nested multi-thread runtime mid-async and panic
        // (`Cannot drop a runtime in an async context`), making Ctrl+C slow/noisy.
        let cancel = Some(Arc::clone(&shutdown.flag));
        match node.sync_cancellable(&ibd_targets, ibd_cfg, cancel).await {
            Ok(n) => {
                if shutdown.requested() {
                    warn!(
                        "ibd: catch-up interrupted accepted≈{n} tip={:?}",
                        node.tip_height()
                    );
                } else {
                    // IBD only Ok-exits on true catch-up (or cancel). Mid-chain
                    // peer death returns Err so we never materialize tip indexes early.
                    // Defense: never claim complete at genesis tip with zero accepts
                    // (stall-exit regression used to enter tip mode at height 0).
                    let tip = node.tip_height().unwrap_or(0);
                    if tip == 0 && n == 0 {
                        warn!(
                            "ibd: returned ok with tip=0 accepted=0 — treating as incomplete (no tip mode)"
                        );
                        catch_up_complete = false;
                    } else {
                        info!("ibd: catch-up accepted≈{n} tip={:?}", node.tip_height());
                        catch_up_complete = true;
                    }
                }
            }
            Err(e) => {
                if shutdown.requested() {
                    warn!("signal: IBD cancelled ({e})");
                } else {
                    // No alternate sync path: restart resumes catch-up indexes.
                    warn!(
                        "ibd: incomplete: {e}; tip={:?} — keeping catch-up indexes (no tip mode; restart to resume)",
                        node.tip_height()
                    );
                }
            }
        }
        // Pull learned peers/flags from IBD back into local book + disk.
        if let Ok(g) = shared_peers.lock() {
            addrman = g.clone();
        }
        if let Err(e) = addrman.save(&peers_path) {
            warn!("peers: save {}: {e}", peers_path.display());
        } else {
            info!(
                "peers: saved {} address(es) to {}",
                addrman.len(),
                peers_path.display()
            );
        }
    } else if ibd_targets.is_empty() {
        info!("ibd: no outbound peers; serving only (use --connect or seeds)");
        catch_up_complete = true;
    }

    // ── Steady state: tip tracking + block relay ────────────────────────────
    // After true catch-up: SH bulk materialize + IndexMode::Tip, then long-lived
    // follow peers (inv/headers + announce). `tx.head` and spend annotations are
    // already correct from Direct IBD — tip entry does not re-scan Class A.
    //
    // Only when catch_up_complete: mid-chain peer death must not enter tip mode
    // (would bulk-load SH while still behind horizon).
    // Tip SH materialize must succeed before follow peers / Electrum. A failed
    // materialize (e.g. ENOSPC mid-cold) leaves reinit'd or residual SH — do not
    // accept tip blocks or serve history until finalize is tip-ready.
    let mut tip_indexes_ready = false;
    if catch_up_complete && !shutdown.requested() {
        let sh_ok = enter_tip_mode(&node.hub.query, Some(Arc::clone(&shutdown.flag)));
        if sh_ok && !shutdown.requested() {
            tip_indexes_ready = true;
            // Tip mode: enable inv/tx accept + announce (P4). Off during IBD by default.
            mempool.set_relay_enabled(true);
            info!(
                "node: catch-up complete tip={:?} — tip tracking + block/tx relay (mempool live={})",
                node.tip_height(),
                mempool.live_count()
            );
        } else if shutdown.requested() {
            warn!(
                "node: tip SH materialize interrupted — restart to resume (CHECKPOINT/READY kept under scripthash.runs/merge/)"
            );
        } else {
            warn!(
                "node: tip SH not ready after materialize — skip follow peers and Electrum; \
                 free disk / fix store and restart to retry finalize"
            );
        }
    } else if !catch_up_complete && !shutdown.requested() {
        warn!(
            "node: catch-up not complete tip={:?} — skip tip mode / Electrum materialize; restart to resume IBD",
            node.tip_height()
        );
    }

    // Persistent follow: stay connected for tip relay after catch-up.
    // Bound each connect so a single dead seed cannot stall post-IBD for minutes
    // (OS TCP timeouts are often 2+ min; IBD dial already uses 8s).
    // Each session actively getheaders from tip (fills gaps after SH materialize).
    if tip_indexes_ready && !shutdown.requested() {
        let follow_n = targets.len().min(max_out.min(3));
        const FOLLOW_CONNECT_SECS: u64 = 8;
        for (i, peer) in targets.iter().take(follow_n).enumerate() {
            if shutdown.requested() {
                break;
            }
            tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    warn!("signal: skip remaining follow connects");
                    break;
                }
                result = tokio::time::timeout(
                    Duration::from_secs(FOLLOW_CONNECT_SECS),
                    node.follow_from(*peer),
                ) => {
                    match result {
                        Ok(Ok(())) => {
                            info!(
                                "node: following peer[{i}] {peer} (live={})",
                                node.follow_live_count()
                            );
                        }
                        Ok(Err(e)) => warn!("node: follow {peer} failed: {e}"),
                        Err(_) => warn!(
                            "node: follow {peer} timed out ({FOLLOW_CONNECT_SECS}s)"
                        ),
                    }
                }
            }
        }
        if node.follow_live_count() == 0 && !targets.is_empty() {
            warn!("node: no follow peers connected — tip announce may stall");
        }
    }

    // Electrum: share Query + mempool; bridge ChainHub tip events → header push.
    // Only when SH tip indexes are ready (same gate as follow peers).
    let mut electrum_handles = Vec::new();
    let mut electrum_bridge = None;
    if tip_indexes_ready {
        if let Some(addr) = config.electrum_listen {
            if !shutdown.requested() {
                let q = node.hub.query.clone();
                let (electrum_tip_tx, _) = broadcast::channel::<TipNotify>(64);
                let mut hub_tips = node.hub.subscribe_tips();
                let bridge_tx = electrum_tip_tx.clone();
                let bridge_stop = Arc::clone(&shutdown.flag);
                electrum_bridge = Some(tokio::spawn(async move {
                    loop {
                        if bridge_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        match hub_tips.recv().await {
                            Ok(ev) => {
                                let mut buf = Vec::with_capacity(80);
                                if ev.header.consensus_encode(&mut buf).is_err() {
                                    continue;
                                }
                                let _ = bridge_tx.send(TipNotify {
                                    height: ev.height,
                                    header_hex: rbitcoin_primitives::hex_encode(buf),
                                });
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }));
                let ecfg = ElectrumConfig::for_params(addr, &params);
                let max_conn = ecfg.limits.max_connections;
                let max_line = ecfg.limits.max_request_bytes;
                let idle_secs = ecfg.limits.idle_timeout.as_secs();
                match run_electrum(
                    ecfg,
                    q,
                    params.clone(),
                    electrum_tip_tx,
                    Some(mempool.clone()),
                )
                .await
                {
                    Ok(h) => {
                        info!(
                            "electrum TCP on {} (Query + mempool; max_conn={} max_line={} idle={}s; TLS via reverse proxy if public)",
                            h.local_addr, max_conn, max_line, idle_secs
                        );
                        electrum_handles.push(h);
                    }
                    Err(e) => warn!("electrum TCP start warning: {e}"),
                }
            }
        }
    }

    // Esplora REST + wallet WebSocket (plain HTTP; TLS via reverse proxy).
    // Tip bridge is independent of Electrum so want:blocks works with Electrum off.
    // Still requires tip SH ready — history endpoints need durable scripthash.
    let mut esplora_handles = Vec::new();
    let mut esplora_tip_bridge = None;
    if tip_indexes_ready {
        if let Some(addr) = config.esplora_listen {
            if !shutdown.requested() {
                let q = node.hub.query.clone();
                let btc_net = match config.network {
                    rbitcoin_primitives::Network::Mainnet => bitcoin::Network::Bitcoin,
                    rbitcoin_primitives::Network::Testnet => bitcoin::Network::Testnet,
                    rbitcoin_primitives::Network::Signet => bitcoin::Network::Signet,
                    rbitcoin_primitives::Network::Regtest => bitcoin::Network::Regtest,
                };
                let (esplora_tip_tx, _) = broadcast::channel::<TipEvent>(64);
                let mut hub_tips = node.hub.subscribe_tips();
                let bridge_tx = esplora_tip_tx.clone();
                let bridge_stop = Arc::clone(&shutdown.flag);
                esplora_tip_bridge = Some(tokio::spawn(async move {
                    loop {
                        if bridge_stop.load(Ordering::SeqCst) {
                            break;
                        }
                        match hub_tips.recv().await {
                            Ok(ev) => {
                                let _ = bridge_tx.send(ev);
                            }
                            Err(broadcast::error::RecvError::Lagged(_)) => continue,
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                }));
                let ecfg = EsploraConfig::with_network(addr, btc_net);
                let max_conn = ecfg.limits.max_connections;
                let max_body = ecfg.limits.max_request_bytes;
                let idle_secs = ecfg.limits.idle_timeout.as_secs();
                let max_ws = ecfg.max_ws_connections;
                match run_esplora(ecfg, q, Some(mempool.clone()), Some(esplora_tip_tx)).await {
                    Ok(h) => {
                        info!(
                        "esplora HTTP+WS on {} (REST + /v1/ws; max_conn={} max_body={} idle={}s max_ws={}; TLS via reverse proxy if public)",
                        h.local_addr, max_conn, max_body, idle_secs, max_ws
                    );
                        esplora_handles.push(h);
                    }
                    Err(e) => warn!("esplora HTTP start warning: {e}"),
                }
            }
        }
    } // tip_indexes_ready (esplora)

    // Tip-follow loop until max_run (`Some(0)` = exit after catch-up + tip mode)
    // or until a shutdown signal.
    //
    // Quiet like Core: log **tip updates** only; when tip looks stale open an
    // extra outbound (log that). No periodic "at tip" heartbeat.
    // Requires tip SH ready — otherwise stay idle (or exit) until restart.
    if tip_indexes_ready && config.max_run_secs != Some(0) && !shutdown.requested() {
        let deadline = config
            .max_run_secs
            .map(|s| Instant::now() + Duration::from_secs(s));
        let mut last_tip = node.tip_height().unwrap_or(0);
        let mut seed_offset = targets.len().min(max_out.min(3));
        let started = Instant::now();
        let mut last_tip_change = Instant::now();
        // How long tip may sit still before dialing an extra peer for a higher tip.
        // Signet ~10m blocks; avoid thrashing.
        const STALE_TIP_SECS: u64 = 600;
        // How often to re-check staleness when no tip event arrives.
        const STALE_POLL_SECS: u64 = 60;
        // DEBUG tip: perf cadence (mirror IBD 5s sample-and-reset).
        const TIP_PERF_SECS: u64 = 5;
        let mut tip_rx = node.hub.subscribe_tips();
        let mut perf_tick = tokio::time::interval(Duration::from_secs(TIP_PERF_SECS));
        perf_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately — skip so window is a full 5s.
        perf_tick.tick().await;
        let mut window_blocks: u64 = 0;

        loop {
            if shutdown.requested() {
                break;
            }
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    break;
                }
            }

            enum Wake {
                Tip(TipEvent),
                Poll,
                Perf,
                Stop,
            }
            // Prefer shutdown, then the 5s perf tick when both ready. Do **not**
            // put tip_rx ahead of perf under `biased` — multi-block catch-up can
            // keep tip events always ready and starve meters (no tip: perf lines).
            let wake = tokio::select! {
                biased;
                _ = shutdown.cancelled() => Wake::Stop,
                _ = perf_tick.tick() => Wake::Perf,
                ev = tip_rx.recv() => match ev {
                    Ok(e) => Wake::Tip(e),
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Missed events — poll tip height below.
                        Wake::Poll
                    }
                    Err(broadcast::error::RecvError::Closed) => Wake::Stop,
                },
                _ = tokio::time::sleep(Duration::from_secs(STALE_POLL_SECS)) => Wake::Poll,
            };
            if matches!(wake, Wake::Stop) {
                break;
            }

            if matches!(wake, Wake::Perf) {
                // Always sample-and-reset so DEBUG-off windows do not accumulate.
                let mp = mempool.sample_reset_perf();
                let (esp_n, esp_us, esp_max) = rbitcoin_esplora::sample_reset_perf();
                let (el_n, el_us, el_max) = rbitcoin_electrum::sample_reset_perf();
                let blks = std::mem::take(&mut window_blocks);
                if enabled(Level::Debug) {
                    let live = mempool.live_count();
                    let follow_live = node.follow_live_count();
                    let acc_avg = if mp.accepts + mp.rejects > 0 {
                        mp.accept_us / (mp.accepts + mp.rejects)
                    } else if mp.accept_us > 0 {
                        mp.accept_us
                    } else {
                        0
                    };
                    let esp_avg = if esp_n > 0 { esp_us / esp_n } else { 0 };
                    let el_avg = if el_n > 0 { el_us / el_n } else { 0 };
                    debug!(
                        "tip: perf follow_live={follow_live} blocks={blks} \
                         mempool live={live} accepts={} rejects={} accept_avg_us={acc_avg} \
                         accept_max_us={} inv_tx={} getdata_tx={} announce={} \
                         esplora req={esp_n} avg_us={esp_avg} max_us={esp_max} \
                         electrum req={el_n} avg_us={el_avg} max_us={el_max}",
                        mp.accepts,
                        mp.rejects,
                        mp.accept_max_us,
                        mp.inv_tx,
                        mp.getdata_tx,
                        mp.announce
                    );
                }
                continue;
            }

            let tip = node.tip_height().unwrap_or(0);
            let elapsed = started.elapsed().as_secs().max(1);
            let delta = tip.saturating_sub(start_tip);

            let follow_live = node.follow_live_count();
            if let Wake::Tip(ev) = &wake {
                // Prefer event height when present (same as store tip after accept).
                let h = ev.height;
                if h != last_tip {
                    // No blk/s: tip-follow only validates blocks as peers deliver them.
                    info!(
                        "node: tip={h} (+{delta} since start, elapsed {elapsed}s, follow_live={follow_live})"
                    );
                    window_blocks =
                        window_blocks.saturating_add(h.saturating_sub(last_tip) as u64);
                    last_tip = h;
                    last_tip_change = Instant::now();
                }
                continue;
            }

            // Poll path: tip advance without event (shouldn't be common), or staleness.
            if tip != last_tip {
                info!(
                    "node: tip={tip} (+{delta} since start, elapsed {elapsed}s, follow_live={follow_live})"
                );
                window_blocks =
                    window_blocks.saturating_add(tip.saturating_sub(last_tip) as u64);
                last_tip = tip;
                last_tip_change = Instant::now();
                continue;
            }

            let stagnant = last_tip_change.elapsed() >= Duration::from_secs(STALE_TIP_SECS);
            if !stagnant || config.connect.is_empty() == false || !config.use_seeds {
                continue;
            }
            if addrman.is_empty() || shutdown.requested() {
                continue;
            }

            // Stale tip: dial one more outbound. New sessions always getheaders from
            // our locator (existing live sessions also re-poll every 2m).
            last_tip_change = Instant::now(); // rate-limit reconnect attempts
            let extra = addrman.take_outbound_offset(1, seed_offset);
            seed_offset = seed_offset.saturating_add(1);
            for peer in extra {
                if shutdown.requested() {
                    break;
                }
                if catch_up_complete {
                    info!(
                        "node: tip may be stale (height={tip}, no update ≥{STALE_TIP_SECS}s, follow_live={follow_live}) — connecting {peer} for a higher tip"
                    );
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = tokio::time::timeout(
                            Duration::from_secs(8),
                            node.follow_from(peer),
                        ) => {
                            match result {
                                Ok(Ok(())) => {
                                    info!(
                                        "node: added follow peer {peer} (follow_live={})",
                                        node.follow_live_count()
                                    );
                                }
                                Ok(Err(e)) => warn!("node: stale-tip peer {peer} failed: {e}"),
                                Err(_) => warn!("node: stale-tip peer {peer} connect timed out"),
                            }
                        }
                    }
                } else {
                    // Catch-up never finished cleanly — re-run IBD against this peer
                    // (never enter tip mode from a partial download).
                    info!("ibd: retry catch-up from {peer} (tip stagnant, catch-up incomplete)");
                    let retry_cfg = catch_up_retry_config(std::sync::Arc::clone(&shared_peers));
                    let cancel = Some(Arc::clone(&shutdown.flag));
                    let retry_peers = [peer];
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = node.sync_cancellable(&retry_peers, retry_cfg, cancel) => {
                            match result {
                                Ok(n) if n > 0 => {
                                    info!(
                                        "ibd: retry got {n} tip={:?} — still catch-up (no tip mode until full catch-up)",
                                        node.tip_height()
                                    );
                                }
                                Ok(_) => {}
                                Err(e) => info!("ibd: retry {peer}: {e}"),
                            }
                        }
                    }
                }
            }
        }
    }

    {
        let end_tip = node.tip_height().unwrap_or(0);
        let blocks_this_run = end_tip.saturating_sub(start_tip);
        let uptime = run_started.elapsed();
        let uptime_secs = uptime.as_secs_f64().max(1e-9);
        let blocks_per_hour = (blocks_this_run as f64) * 3600.0 / uptime_secs;
        info!(
            "node: shutting down tip={end_tip:?} (+{blocks_this_run} blocks this run, \
             uptime={uptime:?}, ~{blocks_per_hour:.1} blk/h)"
        );
    }

    // Final peer book flush (tip-follow may have used stale clone; re-sync from shared).
    if let Ok(g) = shared_peers.lock() {
        addrman.merge_from(&g);
    }
    if let Err(e) = addrman.save(&peers_path) {
        warn!("peers: final save {}: {e}", peers_path.display());
    } else {
        info!(
            "peers: saved {} address(es) to {}",
            addrman.len(),
            peers_path.display()
        );
    }

    for e in electrum_handles {
        e.shutdown().await;
    }
    if let Some(h) = electrum_bridge {
        h.abort();
        let _ = h.await;
    }
    for e in esplora_handles {
        e.shutdown().await;
    }
    if let Some(h) = esplora_tip_bridge {
        h.abort();
        let _ = h.await;
    }
    // Host-friendly: fsync tip tables; MS_ASYNC Class A.
    // Full multi‑GiB fdatasync froze the desktop for 1–2+ minutes on exit.
    if let Err(e) = node.hub.query.flush_for_shutdown() {
        warn!("node: flush warning: {e}");
    } else {
        info!("node: store flushed (shutdown-friendly)");
    }
    if let Err(e) = mempool.flush() {
        warn!("node: mempool flush warning: {e}");
    } else {
        info!(
            "node: mempool flushed gen={} live={}",
            mempool.generation(),
            mempool.live_count()
        );
    }
    node.shutdown().await;
    info!("node: clean exit");
    Ok(())
}

fn should_resolve_default_seeds(config: &NodeConfig) -> bool {
    config.use_seeds && config.connect.is_empty() && config.signet_challenge.is_none()
}

/// Enter steady-state tip mode after true catch-up.
///
/// **Preconditions (enforced by IBD, not repaired here):** Direct catch-up already
/// wrote durable **`tx.head`** (archive) and **spend annotations** (confirm).
/// Incomplete IBD must not call this (`catch_up_complete` only after full horizon).
///
/// **Work here:** the only deferred Direct index is **scripthash** — merge remaining
/// runs and cold bulk-load durable SH tables, then flip [`IndexMode::Tip`].
///
/// No automatic `tx.head` / spend backfill: those paths are recovery tools
/// ([`Query::backfill_tx_index`] for future head rehash rebuild; spend rewrite is
/// not part of tip entry). Corrupt head/spends ⇒ reindex, not silent tip repair.
/// Enter tip index mode after SH materialize.
///
/// Returns `false` if SH materialize failed or was cancelled (do not treat as
/// Electrum-ready). On SIGINT mid-reduce, a fan-in **CHECKPOINT** is left so the
/// next process resumes from the last completed pass.
pub(crate) fn enter_tip_mode(query: &Query, cancel: Option<Arc<AtomicBool>>) -> bool {
    // Fast path: already materialized and watermarks cover tip (near-tip restart).
    if query.sh_is_tip_ready() {
        let _ = query.sync_sh_seal_from_include_hwm();
        query.enter_tip_index_mode();
        info!(
            "node: scripthash tip-ready — skip bulk materialize; mode={:?} rows={}",
            query.index_mode(),
            query.scripthash_entry_count()
        );
        info!("node: tip-mode complete — safe to start Electrum");
        return true;
    }

    // SH: Direct IBD only flush/merges runs; tip does cold bulk-load.
    // Retries incomplete `*.run.mat` claims / CHECKPOINT / READY from crash/SIGINT.
    info!("node: scripthash bulk materialize from runs (merge + cold load)…");
    let cancel_ref = cancel.as_deref();
    let sh_ok = match query.finalize_sh_runs_cancellable(cancel_ref) {
        Ok(n) => {
            info!("node: scripthash bulk materialize creates≈{n}");
            true
        }
        Err(StoreError::Cancelled(msg)) => {
            warn!("node: scripthash bulk materialize cancelled ({msg})");
            warn!(
                "node: partial fan-in progress kept (merge/CHECKPOINT or READY) — \
                 restart to resume; Electrum not ready yet"
            );
            false
        }
        Err(e) => {
            warn!("node: scripthash bulk materialize failed: {e}");
            warn!(
                "node: Electrum history incomplete until materialize succeeds — \
                 keep store/scripthash.runs (incl. *.run.mat / merge/) and restart; \
                 finalize will reinit SH tables and cold-load from claims"
            );
            false
        }
    };
    if !sh_ok {
        return false;
    }

    // Fail closed when Class A exists: residual runs mean creates not yet in
    // durable SH. Empty / genesis-only store has tip_max=0 and is never
    // "tip-ready" by that metric — still allow Tip index flags.
    let tip_max = query.store().txs.count();
    let leftover = query.scripthash_run_count();
    if leftover > 0 {
        warn!(
            "node: scripthash still has {leftover} on-disk run(s) after materialize — \
             refusing tip-follow / Electrum until drain succeeds (restart finalize)"
        );
        return false;
    }
    if tip_max > 0 && !query.sh_is_tip_ready() {
        warn!("node: scripthash materialize returned ok but not tip-ready — refusing tip mode");
        return false;
    }

    query.enter_tip_index_mode();
    info!(
        "node: IndexMode::Tip (tx.head + spend annotations already live; SH durable) mode={:?}",
        query.index_mode()
    );
    info!(
        "node: scripthash rows={} (thin creates from runs; spentness = confirmed-strong annotations)",
        query.scripthash_entry_count()
    );

    info!("node: tip-mode complete — safe to start Electrum");
    true
}

/// Production IBD knobs for a single-peer catch-up retry (stale tip, incomplete catch-up).
///
/// Uses [`IbdConfig::default`] (window 1024, stall 30s, connect 8s, …) — not
/// [`IbdConfig::for_test`], which is only for unit/integration test harnesses.
fn catch_up_retry_config(peers: std::sync::Arc<std::sync::Mutex<AddrMan>>) -> IbdConfig {
    IbdConfig {
        target_peers: 1,
        peers: Some(peers),
        ..IbdConfig::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn catch_up_retry_config_uses_production_not_for_test() {
        let peers = std::sync::Arc::new(std::sync::Mutex::new(rbitcoin_net::AddrMan::new()));
        let cfg = catch_up_retry_config(std::sync::Arc::clone(&peers));
        let prod = IbdConfig::default();
        let test = IbdConfig::for_test();

        assert_eq!(cfg.target_peers, 1);
        assert!(cfg.peers.is_some());
        // Production class (main catch-up path), not for_test knobs.
        assert_eq!(cfg.window, prod.window);
        assert_eq!(cfg.window, rbitcoin_net::DEFAULT_IBD_WINDOW);
        assert_eq!(cfg.per_peer, prod.per_peer);
        assert_eq!(cfg.stall, prod.stall);
        assert_eq!(cfg.connect_timeout, prod.connect_timeout);
        assert_eq!(cfg.headers_batch, prod.headers_batch);
        // Guard against reintroducing for_test() base fields.
        assert_ne!(cfg.window, test.window);
        assert_ne!(cfg.stall, test.stall);
        assert_ne!(cfg.connect_timeout, test.connect_timeout);
    }

    #[test]
    fn custom_signet_does_not_use_default_signet_seeds() {
        let mut cfg = NodeConfig::default().with_network(rbitcoin_primitives::Network::Signet);
        assert!(should_resolve_default_seeds(&cfg));
        cfg.signet_challenge = Some(bitcoin::ScriptBuf::from_bytes(vec![0x51]));
        assert!(!should_resolve_default_seeds(&cfg));
    }

    #[test]
    fn enter_tip_mode_reenables_indexes() {
        use rbitcoin_query::IndexMode;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-tip-mode-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.enter_direct_index_mode().unwrap();
        assert_eq!(q.index_mode(), IndexMode::Direct);
        assert!(q.spend_index_enabled());
        assert!(q.tx_index_enabled());

        enter_tip_mode(&q, None);
        assert_eq!(q.index_mode(), IndexMode::Tip);
        assert!(q.spend_index_enabled());
        assert!(q.tx_index_enabled());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shutdown_flag_and_node_handle_smoke() {
        let sd = Shutdown::new();
        assert!(!sd.requested());
        sd.request();
        assert!(sd.requested());
        // Second request is idempotent.
        sd.request();
        assert!(sd.requested());

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-node-{nanos}"));
        let cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest);
        let handle = run_node(cfg).expect("run_node");
        assert_eq!(handle.network_name(), "regtest");
        assert!(handle.mempool.is_none());
        let _ = format!("{:?}", handle);
        handle.shutdown().expect("shutdown flush");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_p2p_no_peers_exits_after_catchup() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-{nanos}"));
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        cfg.connect.clear();
        cfg.max_run_secs = Some(0); // exit after catch-up / tip mode
        cfg.smoke = false;
        // Bound runtime so a hang fails the test suite instead of blocking.
        // max_run_secs=0 should exit immediately after catch-up; keep bound tight.
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        result.unwrap().expect("run_p2p ok with no peers");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancelled_completes_after_request() {
        let sd = Shutdown::new();
        // Already-requested path returns immediately.
        sd.request();
        sd.cancelled().await;

        let sd2 = Shutdown::new();
        let s2 = Arc::clone(&sd2);
        let j = tokio::spawn(async move {
            s2.cancelled().await;
        });
        // Give the task a chance to park on notify.
        tokio::task::yield_now().await;
        sd2.request();
        j.await.unwrap();
    }

    #[tokio::test]
    async fn run_p2p_milestone_and_electrum() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-el-{nanos}"));
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        cfg.connect.clear();
        cfg.milestone_height = 100; // exercise milestone log branch
        cfg.electrum_listen = Some("127.0.0.1:0".parse().unwrap());
        // max_run_secs=0 exits after catch-up/tip (tip-follow loop uses 60s poll sleeps).
        cfg.max_run_secs = Some(0);
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        result.unwrap().expect("run_p2p with electrum");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_p2p_with_esplora_listen() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-esp-{nanos}"));
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        cfg.connect.clear();
        cfg.esplora_listen = Some("127.0.0.1:0".parse().unwrap());
        cfg.max_run_secs = Some(0);
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        result.unwrap().expect("run_p2p with esplora");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_p2p_bad_connect_peer_still_exits() {
        // Explicit dead --connect so IBD/follow attempts are exercised, then exit.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-conn-{nanos}"));
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        // Blackhole / closed port: connect fails fast under FOLLOW_CONNECT_SECS.
        cfg.connect = vec!["127.0.0.1:1".parse().unwrap()];
        cfg.max_run_secs = Some(0);
        // Dead connect should fail fast (FOLLOW_CONNECT_SECS); 20s bound for hang detection.
        let result = tokio::time::timeout(Duration::from_secs(20), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        // Incomplete IBD is ok (warn path); should not hang.
        let _ = result.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn enter_tip_mode_warns_on_leftover_runs_dir() {
        use rbitcoin_query::IndexMode;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-tip-leftover-{nanos}"));
        std::fs::create_dir_all(dir.join("store")).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.enter_direct_index_mode().unwrap();
        // Empty store: finalize has no runs; still flips to tip.
        enter_tip_mode(&q, None);
        assert_eq!(q.index_mode(), IndexMode::Tip);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn node_handle_shutdown_with_mempool() {
        use rbitcoin_net::MempoolHub;
        use std::sync::Arc;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-handle-mp-{nanos}"));
        let cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest);
        let mut handle = run_node(cfg).expect("run_node");
        // Dual-open same store under /tmp for MempoolHub's Arc<Query> (flush only).
        let q = Arc::new(Query::open_or_create(handle.config.store_path()).unwrap());
        let mp = MempoolHub::open(handle.config.mempool_path(), q).expect("mempool");
        handle.mempool = Some(mp);
        let _ = format!("{:?}", handle);
        handle.shutdown().expect("flush query+mempool");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_p2p_with_peers_file_and_electrum() {
        use rbitcoin_net::AddrMan;
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-peers-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        // Non-empty peers book so load path logs address count.
        let mut am = AddrMan::new();
        am.add("127.0.0.1:18444".parse().unwrap());
        am.add("127.0.0.1:18445".parse().unwrap());
        am.save(&dir.join("peers")).unwrap();

        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        // Peers file is loaded for bookkeeping; do not dial those addrs as --connect
        // (would stall IBD). Empty connect + no seeds → catch-up complete immediately.
        cfg.connect.clear();
        cfg.max_run_secs = Some(0);
        cfg.electrum_listen = Some("127.0.0.1:0".parse().unwrap());
        cfg.milestone_height = 50;
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        result.unwrap().expect("run_p2p peers+electrum");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn run_p2p_corrupt_peers_and_dead_connect() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-badpeers-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        // Corrupt peers file → load error branch starts empty book.
        std::fs::write(dir.join("peers"), b"not-a-valid-peers-blob\xff\x00").unwrap();

        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        cfg.connect = vec!["127.0.0.1:1".parse().unwrap()];
        cfg.max_run_secs = Some(0);
        let result = tokio::time::timeout(Duration::from_secs(20), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        let _ = result.unwrap(); // incomplete IBD ok
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn cancelled_waits_for_request_race() {
        // Cover the while !requested re-check after spurious notify.
        let sd = Shutdown::new();
        let s = Arc::clone(&sd);
        let j = tokio::spawn(async move {
            s.cancelled().await;
        });
        tokio::task::yield_now().await;
        // Double request is idempotent; first wakes waiters.
        sd.request();
        sd.request();
        j.await.unwrap();
    }

    /// `use_seeds=true` on regtest resolves empty seed set (covers seed inject path).
    #[tokio::test]
    async fn run_p2p_use_seeds_regtest_empty() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-seeds-{nanos}"));
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = true; // regtest: resolve_all_seeds → empty
        cfg.connect.clear();
        cfg.max_run_secs = Some(0);
        cfg.milestone_height = 1; // log milestone branch
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        result.unwrap().expect("run_p2p seeds regtest");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Electrum bind failure (port already taken / invalid) → warn path, still exits.
    #[tokio::test]
    async fn run_p2p_electrum_bind_fail_warns() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-run-p2p-el-fail-{nanos}"));
        // Hold a port so electrum bind fails.
        let held = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = held.local_addr().unwrap();
        let mut cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(rbitcoin_primitives::Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        cfg.use_seeds = false;
        cfg.connect.clear();
        cfg.electrum_listen = Some(addr); // already bound → fail
        cfg.max_run_secs = Some(0);
        let result = tokio::time::timeout(Duration::from_secs(15), run_p2p(cfg)).await;
        assert!(result.is_ok(), "run_p2p timed out");
        // Bind fail is non-fatal warn; run should still complete.
        result.unwrap().expect("run_p2p despite electrum fail");
        drop(held);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
