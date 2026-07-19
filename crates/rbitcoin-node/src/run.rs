use crate::config::NodeConfig;
use crate::error::NodeError;
use bitcoin::consensus::Encodable;
use rbitcoin_consensus::ChainParams;
use rbitcoin_electrum::{run_electrum, run_electrum_tls, ElectrumConfig, TipNotify};
use rbitcoin_net::{default_port, AddrMan, IbdConfig, MempoolHub, P2PNode, TipEvent};
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
    let params = ChainParams::for_network(config.network);
    let milestone = config.milestone();
    if milestone.height > 0 {
        info!(
            "ibd: milestone height={} (script/sig checks skipped at/below; prevouts always; durable points deferred until tip mode)",
            milestone.height
        );
    }
    // Thin scripthash creates (outpoint pointers) always written on Class C.
    // Spentness is joined from points + Class C at Electrum query time — no
    // per-spend scripthash RMW (that used to stall confirm).
    // Direct head writes (no overlay); SH runs in parallel with strong/height.
    info!("ibd: thin scripthash creates always ON (confirm path; direct head writes)");
    // Point spends: durable multimap off under milestone (archive + confirm). Confirm
    // still tracks spends process-locally for double-spend checks; durable points
    // are backfilled in enter_tip_mode before Electrum starts.
    let spend_index = milestone.height == 0;
    handle.query.set_spend_index(spend_index);
    if !spend_index {
        info!(
            "ibd: durable point/spend index OFF during catch-up (local spent-set only; backfill before Electrum)"
        );
    } else {
        // Full validation: point.head is multi‑GiB random RMW. Buffer upserts in RAM
        // and spill slot-sorted/page-buffered batches (default 512k keys ≈ tens of MiB).
        let cap = std::env::var("RBITCOIN_POINT_HEAD_OVERLAY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(rbitcoin_store::DEFAULT_WRITE_BEHIND_CAP);
        handle
            .query
            .enable_point_head_write_behind(cap)
            .map_err(|e| NodeError::Config(format!("point head write-behind: {e}")))?;
        info!(
            "ibd: point.head write-behind ON (cap={cap}; budgeted spill chunk≈{}; RBITCOIN_POINT_HEAD_OVERLAY / RBITCOIN_HEAD_SPILL_CHUNK)",
            rbitcoin_store::spill_chunk_size()
        );
    }
    // tx.head: optional under milestone for archive speed; connect uses prev_tx_fk
    // + process txid→fk cache. On resume with head off, warm that cache from Class A
    // so external-prev inputs (out-of-order archive) still resolve.
    let tx_index = milestone.height == 0;
    handle.query.set_tx_index(tx_index);
    if !tx_index {
        let t0 = Instant::now();
        let n = handle
            .query
            .warm_txid_cache_from_bodies()
            .map_err(|e| NodeError::Config(format!("warm txid cache: {e}")))?;
        info!(
            "ibd: txid hash-head OFF during catch-up (prevouts via prev_tx_fk + process cache; warmed {n} Class A txs in {:?})",
            t0.elapsed()
        );
    } else {
        let cap = std::env::var("RBITCOIN_TX_HEAD_OVERLAY")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(rbitcoin_store::DEFAULT_WRITE_BEHIND_CAP);
        handle
            .query
            .enable_tx_head_write_behind(cap)
            .map_err(|e| NodeError::Config(format!("tx head write-behind: {e}")))?;
        info!(
            "ibd: tx.head write-behind ON (cap={cap}; budgeted spill chunk≈{}; RBITCOIN_TX_HEAD_OVERLAY / RBITCOIN_HEAD_SPILL_CHUNK)",
            rbitcoin_store::spill_chunk_size()
        );
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
        // Prefer a wide seed sample (IPv4-first) for parallel dial / redial.
        addrman.take_outbound(candidate_n)
    };
    // True once parallel (or sequential fallback) catch-up finished without cancel.
    // Steady-state tip follow must **not** keep opening sequential re-IBD sessions.
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
            ..IbdConfig::default()
        };
        info!(
            "ibd: parallel catch-up candidates={} target_peers={} (window={}, per_peer={})…",
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
        match node
            .parallel_sync_cancellable(&ibd_targets, ibd_cfg, cancel)
            .await
        {
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
                    catch_up_complete = true;
                }
            }
            Err(e) => {
                if shutdown.requested() {
                    warn!("signal: parallel IBD cancelled ({e})");
                } else {
                    warn!("ibd: parallel catch-up warning: {e}; falling back sequential");
                    if !shutdown.requested() {
                        tokio::select! {
                            biased;
                            _ = shutdown.cancelled() => {
                                warn!("signal: interrupting sequential fallback…");
                            }
                            result = node.sync_from_peers(&ibd_targets) => {
                                match result {
                                    Ok(n) => {
                                        info!(
                                            "ibd: sequential fallback downloaded≈{n} tip={:?}",
                                            node.tip_height()
                                        );
                                        catch_up_complete = true;
                                    }
                                    Err(e2) => error!("ibd: sequential also failed: {e2}"),
                                }
                            }
                        }
                    }
                }
            }
        }
    } else if ibd_targets.is_empty() {
        info!("ibd: no outbound peers; serving only (use --connect or seeds)");
        catch_up_complete = true;
    }

    // ── Steady state: tip tracking + block relay ────────────────────────────
    // After catch-up we re-enable indexes that were off for IBD speed, open
    // long-lived follow peers (inv/headers + announce), and stop thrashing
    // sequential re-IBD every progress tick (signet log: "retry catch-up"
    // every 10s forever while already at peer tip).
    if catch_up_complete && !shutdown.requested() {
        enter_tip_mode(&node.hub.query);
        // Tip mode: enable inv/tx accept + announce (P4). Off during IBD by default.
        mempool.set_relay_enabled(true);
        info!(
            "node: catch-up complete tip={:?} — tip tracking + block/tx relay (mempool live={})",
            node.tip_height(),
            mempool.live_count()
        );
    }

    // Persistent follow: stay connected for tip relay after catch-up.
    // Bound each connect so a single dead seed cannot stall post-IBD for minutes
    // (OS TCP timeouts are often 2+ min; parallel IBD already uses 8s).
    let mut follow_peers = 0usize;
    if !shutdown.requested() {
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
                            info!("node: following peer[{i}] {peer}");
                            follow_peers += 1;
                        }
                        Ok(Err(e)) => warn!("node: follow {peer} failed: {e}"),
                        Err(_) => warn!(
                            "node: follow {peer} timed out ({FOLLOW_CONNECT_SECS}s)"
                        ),
                    }
                }
            }
        }
        if follow_peers == 0 && !targets.is_empty() {
            warn!("node: no follow peers connected — tip announce may stall");
        }
    }

    // Electrum: share Query + mempool; bridge ChainHub tip events → header push.
    let mut electrum_handles = Vec::new();
    let mut electrum_bridge = None;
    let want_tcp = config.electrum_listen.is_some();
    let want_tls = config.electrum_tls_listen.is_some();
    if (want_tcp || want_tls) && !shutdown.requested() {
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
        if let Some(addr) = config.electrum_listen {
            let ecfg = ElectrumConfig::for_params(addr, &params);
            match run_electrum(
                ecfg,
                q.clone(),
                params.clone(),
                electrum_tip_tx.clone(),
                Some(mempool.clone()),
            )
            .await
            {
                Ok(h) => {
                    info!(
                        "electrum TCP on {} (Query + mempool broadcast/unconf/fees)",
                        h.local_addr
                    );
                    electrum_handles.push(h);
                }
                Err(e) => warn!("electrum TCP start warning: {e}"),
            }
        }
        if let Some(addr) = config.electrum_tls_listen {
            let cert = config
                .electrum_tls_cert
                .clone()
                .expect("validated in cli");
            let key = config.electrum_tls_key.clone().expect("validated in cli");
            let ecfg = ElectrumConfig::for_params(addr, &params);
            match run_electrum_tls(
                ecfg,
                q,
                params.clone(),
                electrum_tip_tx,
                Some(mempool.clone()),
                cert,
                key,
            )
            .await
            {
                Ok(h) => {
                    info!("electrum TLS on {} (PEM cert/key)", h.local_addr);
                    electrum_handles.push(h);
                }
                Err(e) => warn!("electrum TLS start warning: {e}"),
            }
        }
    }

    // Tip-follow loop until max_run (`Some(0)` = exit after catch-up + tip mode)
    // or until a shutdown signal.
    //
    // Quiet like Core: log **tip updates** only; when tip looks stale open an
    // extra outbound (log that). No periodic "at tip" heartbeat.
    if config.max_run_secs != Some(0) && !shutdown.requested() {
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
        let mut tip_rx = node.hub.subscribe_tips();

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
                Stop,
            }
            let wake = tokio::select! {
                biased;
                _ = shutdown.cancelled() => Wake::Stop,
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

            let tip = node.tip_height().unwrap_or(0);
            let elapsed = started.elapsed().as_secs().max(1);
            let delta = tip.saturating_sub(start_tip);

            if let Wake::Tip(ev) = &wake {
                // Prefer event height when present (same as store tip after accept).
                let h = ev.height;
                if h != last_tip {
                    let rate = delta as f64 / elapsed as f64;
                    info!(
                        "node: tip={h} (+{delta} since start, ~{rate:.2} blk/s, elapsed {elapsed}s, follow peers={follow_peers})"
                    );
                    last_tip = h;
                    last_tip_change = Instant::now();
                }
                continue;
            }

            // Poll path: tip advance without event (shouldn't be common), or staleness.
            if tip != last_tip {
                let rate = delta as f64 / elapsed as f64;
                info!(
                    "node: tip={tip} (+{delta} since start, ~{rate:.2} blk/s, elapsed {elapsed}s, follow peers={follow_peers})"
                );
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

            // Stale tip: dial one more outbound looking for a higher tip (Core-ish).
            last_tip_change = Instant::now(); // rate-limit reconnect attempts
            let extra = addrman.take_outbound_offset(1, seed_offset);
            seed_offset = seed_offset.saturating_add(1);
            for peer in extra {
                if shutdown.requested() {
                    break;
                }
                if catch_up_complete {
                    info!(
                        "node: tip may be stale (height={tip}, no update ≥{STALE_TIP_SECS}s) — connecting {peer} for a higher tip"
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
                                    follow_peers += 1;
                                    info!("node: added follow peer {peer} (follow peers={follow_peers})");
                                }
                                Ok(Err(e)) => warn!("node: stale-tip peer {peer} failed: {e}"),
                                Err(_) => warn!("node: stale-tip peer {peer} connect timed out"),
                            }
                        }
                    }
                } else {
                    // Catch-up never finished cleanly — full seed re-sync.
                    info!("ibd: retry catch-up from {peer} (tip stagnant, catch-up incomplete)");
                    tokio::select! {
                        biased;
                        _ = shutdown.cancelled() => break,
                        result = node.sync_from(peer) => {
                            match result {
                                Ok(n) if n > 0 => {
                                    info!("ibd: retry got {n} tip={:?}", node.tip_height());
                                    catch_up_complete = true;
                                    enter_tip_mode(&node.hub.query);
                                    mempool.set_relay_enabled(true);
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
        "node: shutting down tip={:?} (+{} blocks this run)",
        node.tip_height(),
        node.tip_height().unwrap_or(0).saturating_sub(start_tip)
    );

    for e in electrum_handles {
        e.shutdown().await;
    }
    if let Some(h) = electrum_bridge {
        h.abort();
        let _ = h.await;
    }
    // Host-friendly: spill head overlays + fsync tip tables; MS_ASYNC Class A.
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

/// Re-enable store indexes that were disabled for IBD speed so tip-follow /
/// Electrum / spend queries work in steady state.
///
/// After milestone catch-up, Class A bodies exist but durable `tx.head` and
/// **point spends** may have been skipped. We **backfill** those here **before
/// Electrum binds**. Thin scripthash creates are always written on confirm;
/// no tip-mode SH rebuild (corrupt index ⇒ reindex).
pub(crate) fn enter_tip_mode(query: &Query) {
    // Spill any IBD write-behind overlays and switch to write-through so tip
    // follow is immediately durable (low write rate; no need to buffer).
    if let Err(e) = query.disable_point_head_write_behind() {
        warn!("node: disable point head write-behind: {e}");
    }
    if let Err(e) = query.disable_tx_head_write_behind() {
        warn!("node: disable tx head write-behind: {e}");
    }
    // Always on at tip: point spends + txid head (prevout / maturity / reorg).
    query.set_spend_index(true);
    query.set_tx_index(true);
    info!("node: tip mode indexes spend=on tx_head=on scripthash=always");

    let tip = query.tip_height().map(|h| h.0).unwrap_or(0);

    // 1) tx.head first: scripthash spend joins need get_tx_by_txid when prev_tx_fk is null.
    let bodies = query.tx_body_count();
    let head_before = query.tx_head_occupied();
    // Heuristic: head much smaller than bodies ⇒ archive ran with index off.
    if bodies > 0 && head_before.saturating_mul(2) < bodies {
        info!(
            "node: backfill tx.head starting (bodies={bodies}, head_before={head_before})…"
        );
        let t0 = Instant::now();
        match query.backfill_tx_index(|done, total, inserted| {
            info!(
                "node: backfill tx.head progress {done}/{total} bodies (inserted≈{inserted}, elapsed {:?})",
                t0.elapsed()
            );
        }) {
            Ok(n) => info!(
                "node: backfill tx.head done inserted={n} in {:?}",
                t0.elapsed()
            ),
            Err(e) => warn!("node: backfill tx.head failed: {e}"),
        }
    } else if bodies > 0 {
        // Still warm process cache; head may already be complete.
        let _ = query.warm_txid_cache_from_bodies();
    }

    // 2) Durable point/spend edges (confirm skipped these during milestone IBD).
    let point_before = query.point_edge_count();
    // Empty or sparse points while we confirmed a real tip ⇒ rebuild.
    let need_points = tip > 0 && point_before < tip as u64;
    if need_points {
        info!(
            "node: backfill point spends starting (tip={tip}, edges_before={point_before})…"
        );
        let t0 = Instant::now();
        match query.backfill_point_spends(|h, tip_h, txs, edges| {
            info!(
                "node: backfill point spends progress height={h}/{tip_h} txs={txs} edges≈{edges} (elapsed {:?})",
                t0.elapsed()
            );
        }) {
            Ok((heights, txs)) => info!(
                "node: backfill point spends done heights={heights} txs={txs} edges_now={} in {:?}",
                query.point_edge_count(),
                t0.elapsed()
            ),
            Err(e) => warn!("node: backfill point spends failed: {e}"),
        }
    } else {
        info!(
            "node: point spend index present (edges={point_before}, tip={tip}) — skip backfill"
        );
    }

    // Scripthash is always maintained on confirm (thin creates). No tip-mode
    // rebuild: corrupt/missing index ⇒ reindex like any other table. Rows for
    // unstrong creates are invisible via is_confirmed_strong (kill -9 / reorg).
    info!(
        "node: scripthash rows={} (thin creates; spentness via points)",
        query.scripthash_entry_count()
    );

    info!("node: tip-mode backfill complete — safe to start Electrum");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn enter_tip_mode_reenables_indexes() {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("rbitcoin-tip-mode-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        let q = Query::open_or_create(dir.join("store")).unwrap();
        q.set_spend_index(false);
        q.set_tx_index(false);
        assert!(!q.spend_index_enabled());
        assert!(!q.tx_index_enabled());

        enter_tip_mode(&q);
        assert!(q.spend_index_enabled());
        assert!(q.tx_index_enabled());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
