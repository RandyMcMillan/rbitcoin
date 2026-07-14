use crate::config::NodeConfig;
use crate::error::NodeError;
use rbitcoin_query::Query;
use rbitcoin_wire_cache::WireRing;

/// Running node state (minimal for Phase 0/1).
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

/// Start the node: ensure datadir, open store, prepare placeholders.
pub fn run_node(config: NodeConfig) -> Result<NodeHandle, NodeError> {
    config.ensure_datadir()?;
    let query = Query::open_or_create(config.store_path())?;
    let wire = WireRing::new(config.wire_depth_blocks);
    Ok(NodeHandle {
        config,
        query,
        wire,
    })
}
