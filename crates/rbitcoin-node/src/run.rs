use crate::config::NodeConfig;
use crate::error::NodeError;
use rbitcoin_consensus::{ChainParams, Milestone};
use rbitcoin_net::{default_port, AddrMan, P2PNode};
use rbitcoin_query::Query;
use rbitcoin_wire_cache::WireRing;
use std::net::SocketAddr;
use std::time::Duration;

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

/// Long-running P2P loop: listen (optional), connect peers, sync, then idle until cancelled.
///
/// Used by the process binary when not in `--smoke` mode. Tests can call this with a short
/// `idle` or cancel via dropping the future (timeout).
pub async fn run_p2p(config: NodeConfig) -> Result<(), NodeError> {
    let handle = run_node(config.clone())?;
    let params = ChainParams::for_network(config.network);
    let milestone = Milestone::NONE;

    let listen = config.p2p_listen.unwrap_or_else(|| {
        SocketAddr::from(([127, 0, 0, 1], default_port(config.network)))
    });

    let query = handle.query;
    let node = P2PNode::start(listen, query, params, milestone)
        .await
        .map_err(|e| NodeError::Config(format!("p2p start: {e}")))?;

    eprintln!(
        "rbitcoin-node listening on {} ({})",
        node.local_addr,
        config.network.as_str()
    );

    let mut addrman = AddrMan::new();
    for c in &config.connect {
        addrman.add(*c);
    }
    if config.use_seeds && config.connect.is_empty() {
        addrman = AddrMan::with_seeds(config.network);
    }

    let targets = if !config.connect.is_empty() {
        config.connect.clone()
    } else {
        addrman.take_outbound(rbitcoin_net::outbound_for_ibd(true) as usize)
    };

    if !targets.is_empty() {
        match node.sync_from_peers(&targets).await {
            Ok(n) => eprintln!("synced {n} blocks from peers; tip={:?}", node.tip_height()),
            Err(e) => eprintln!("sync warning: {e}"),
        }
    } else {
        eprintln!("no outbound peers configured; serving only");
    }

    // Idle: keep accept loop alive until max_run or process kill.
    match config.max_run_secs {
        Some(0) => {}
        Some(secs) => {
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }
        None => loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        },
    }

    node.shutdown().await;
    Ok(())
}
