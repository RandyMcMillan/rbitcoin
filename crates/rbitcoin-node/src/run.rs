use crate::config::NodeConfig;
use crate::error::NodeError;
use rbitcoin_consensus::ChainParams;
use rbitcoin_electrum::{run_electrum, ElectrumConfig};
use rbitcoin_net::{default_port, AddrMan, IbdConfig, P2PNode};
use rbitcoin_query::Query;
use rbitcoin_wire_cache::WireRing;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Notify};
use rbitcoin_log::{error, info, warn};

/// Running node state (store open; optional P2P).
pub struct NodeHandle {
    pub config: NodeConfig,
    pub query: Query,
    pub wire: WireRing,
}

impl std::fmt::Debug for NodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHandle")
            .field("config", &self.config)
            .field("network", &self.config.network)
            .finish()
    }
}

impl NodeHandle {
    pub fn network_name(&self) -> &'static str {
        self.config.network.as_str()
    }

    pub fn shutdown(self) -> Result<(), NodeError> {
        self.query.flush()?;
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
    Ok(NodeHandle {
        config,
        query,
        wire,
    })
}

/// Long-running P2P (+ optional Electrum): seed resolve, catch-up, persistent follow, progress logs.
///
/// Cleanly exits on **SIGTERM** / **SIGINT** (`kill <pid>` or Ctrl+C): flushes the store
/// and aborts peer tasks.
pub async fn run_p2p(config: NodeConfig) -> Result<(), NodeError> {
    let handle = run_node(config.clone())?;
    let params = ChainParams::for_network(config.network);
    let milestone = config.milestone();
    if milestone.height > 0 {
        info!(
            "ibd: milestone height={} (script/prevout checks skipped at/below)",
            milestone.height
        );
    }
    // Fast IBD: skip scripthash / spend / txid indexes under milestone (connect
    // checks are also skipped there). Re-enable / reindex before full validation
    // or Electrum. tx.head inserts were the main single-thread archive bottleneck.
    let scripthash = config.scripthash_index && milestone.height == 0;
    handle.query.set_scripthash_index(scripthash);
    if !scripthash {
        info!("ibd: scripthash index OFF during catch-up (re-enable after tip for Electrum)");
    }
    let spend_index = milestone.height == 0;
    handle.query.set_spend_index(spend_index);
    if !spend_index {
        info!("ibd: spend index OFF during catch-up under milestone (reindex before full validation)");
    }
    let tx_index = milestone.height == 0;
    handle.query.set_tx_index(tx_index);
    if !tx_index {
        info!("ibd: txid hash-head OFF during catch-up under milestone (bodies still complete via header_txs)");
    }

    let listen = config
        .p2p_listen
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], default_port(config.network))));

    let start_tip = handle.query.tip_height().map(|h| h.0).unwrap_or(0);
    info!(
        "rbitcoin-node starting network={} datadir={} tip={start_tip}",
        config.network.as_str(),
        config.datadir.display()
    );

    let query = handle.query;
    let mut node = P2PNode::start(listen, query, params.clone(), milestone)
        .await
        .map_err(|e| NodeError::Config(format!("p2p start: {e}")))?;

    info!(
        "rbitcoin-node listening on {} ({})",
        node.local_addr,
        config.network.as_str()
    );

    let shutdown = Shutdown::new();
    spawn_signal_handler(shutdown.clone());

    let mut addrman = AddrMan::new();
    for c in &config.connect {
        addrman.add(*c);
    }
    if config.use_seeds && config.connect.is_empty() {
        info!(
            "ibd: resolving DNS/fixed seeds for {}…",
            config.network.as_str()
        );
        addrman = AddrMan::with_seeds(config.network);
        info!("ibd: {} seed addresses resolved", addrman.len());
    }

    let max_out = config.max_outbound.max(1) as usize;
    // Candidate pool larger than target so dials can fail and still hit target_peers.
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
        // Prefer a wide seed sample (IPv4-first) for parallel dial / redial.
        addrman.take_outbound(candidate_n)
    };
    if !ibd_targets.is_empty() && !shutdown.requested() {
        let target_peers = max_out.clamp(8, 32);
        let ibd_cfg = IbdConfig {
            // Global horizon 1024; per-peer in-transit 16 (Bitcoin Core-class).
            window: rbitcoin_net::DEFAULT_IBD_WINDOW,
            per_peer: rbitcoin_net::DEFAULT_BLOCKS_IN_TRANSIT_PER_PEER,
            target_peers,
            stall: std::time::Duration::from_secs(5),
            ..IbdConfig::default()
        };
        info!(
            "ibd: parallel catch-up candidates={} target_peers={} (window={}, per_peer={})…",
            ibd_targets.len(),
            ibd_cfg.target_peers,
            ibd_cfg.window,
            ibd_cfg.per_peer
        );
        // Cooperative cancel: IBD polls the same AtomicBool the signal handler sets.
        let cancel = Some(Arc::clone(&shutdown.flag));
        tokio::select! {
            biased;
            _ = shutdown.cancelled() => {
                warn!("signal: interrupting parallel IBD…");
            }
            result = node.parallel_sync_cancellable(&ibd_targets, ibd_cfg, cancel) => {
                match result {
                    Ok(n) => {
                        if shutdown.requested() {
                            warn!(
                                "ibd: parallel catch-up interrupted accepted≈{n} tip={:?}",
                                node.tip_height()
                            );
                        } else {
                            info!(
                                "ibd: parallel catch-up accepted≈{n} tip={:?}",
                                node.tip_height()
                            );
                        }
                    }
                    Err(e) => {
                        if shutdown.requested() {
                            warn!("signal: parallel IBD cancelled ({e})");
                        } else {
                            warn!(
                                "ibd: parallel catch-up warning: {e}; falling back sequential"
                            );
                            if !shutdown.requested() {
                                tokio::select! {
                                    biased;
                                    _ = shutdown.cancelled() => {
                                        warn!("signal: interrupting sequential fallback…");
                                    }
                                    result = node.sync_from_peers(&ibd_targets) => {
                                        match result {
                                            Ok(n) => info!(
                                                "ibd: sequential fallback downloaded≈{n} tip={:?}",
                                                node.tip_height()
                                            ),
                                            Err(e2) => error!("ibd: sequential also failed: {e2}"),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    } else if ibd_targets.is_empty() {
        info!("ibd: no outbound peers; serving only (use --connect or seeds)");
    }

    // Persistent follow: stay connected for tip relay after catch-up.
    if !shutdown.requested() {
        let follow_n = targets.len().min(max_out.min(3));
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
                result = node.follow_from(*peer) => {
                    match result {
                        Ok(()) => info!("ibd: following peer[{i}] {peer}"),
                        Err(e) => warn!("ibd: follow {peer} failed: {e}"),
                    }
                }
            }
        }
    }

    let (tip_tx, _) = broadcast::channel(16);
    let mut electrum = None;
    if let Some(addr) = config.electrum_listen {
        if !shutdown.requested() {
            let ecfg = ElectrumConfig::for_params(addr, &params);
            let q =
                Arc::new(Query::open_or_create(config.store_path()).map_err(NodeError::from)?);
            match run_electrum(ecfg, q, params.clone(), tip_tx.clone()).await {
                Ok(h) => {
                    info!("electrum listening on {}", h.local_addr);
                    electrum = Some(h);
                }
                Err(e) => warn!("electrum start warning: {e}"),
            }
        }
    }

    // Progress + optional re-seed loop until max_run (`Some(0)` = exit after catch-up)
    // or until a shutdown signal.
    if config.max_run_secs != Some(0) && !shutdown.requested() {
        let deadline = config
            .max_run_secs
            .map(|s| Instant::now() + Duration::from_secs(s));
        let mut last_tip = node.tip_height().unwrap_or(0);
        let mut seed_offset = targets.len().min(max_out.min(3));
        let started = Instant::now();

        loop {
            if shutdown.requested() {
                break;
            }
            if let Some(d) = deadline {
                if Instant::now() >= d {
                    break;
                }
            }

            tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
            }

            let tip = node.tip_height().unwrap_or(0);
            let elapsed = started.elapsed().as_secs().max(1);
            let delta = tip.saturating_sub(start_tip);
            let rate = delta as f64 / elapsed as f64;
            if tip != last_tip {
                info!(
                    "ibd: tip={tip} (+{delta} since start, ~{rate:.2} blk/s, elapsed {elapsed}s)"
                );
                last_tip = tip;
            } else {
                info!("ibd: tip={tip} (no advance this interval, elapsed {elapsed}s)");
            }

            // If no progress, try another outbound catch-up peer from seeds.
            if tip == last_tip
                && config.connect.is_empty()
                && config.use_seeds
                && !addrman.is_empty()
                && !shutdown.requested()
            {
                let extra = addrman.take_outbound_offset(1, seed_offset);
                seed_offset = seed_offset.saturating_add(1);
                for peer in extra {
                    if shutdown.requested() {
                        break;
                    }
                    info!("ibd: retry catch-up from {peer}");
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = node.sync_from(peer) => {
                            match result {
                                Ok(n) if n > 0 => {
                                    info!("ibd: retry got {n} tip={:?}", node.tip_height());
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

    info!(
        "ibd: shutting down tip={:?} (+{} blocks this run)",
        node.tip_height(),
        node.tip_height().unwrap_or(0).saturating_sub(start_tip)
    );

    if let Some(e) = electrum {
        e.shutdown().await;
    }
    if let Err(e) = node.hub.query.flush() {
        warn!("ibd: flush warning: {e}");
    } else {
        info!("ibd: store flushed");
    }
    node.shutdown().await;
    info!("ibd: clean exit");
    Ok(())
}
