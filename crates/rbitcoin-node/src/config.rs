use crate::error::NodeError;
use rbitcoin_primitives::Network;
use std::path::PathBuf;

/// Node process configuration (CLI / conf file surface grows over time).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    pub datadir: PathBuf,
    pub network: Network,
    pub archive_durability: bool,
    pub wire_depth_blocks: u32,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            datadir: PathBuf::from("./datadir"),
            network: Network::Mainnet,
            archive_durability: true,
            wire_depth_blocks: 100,
        }
    }
}

impl NodeConfig {
    pub fn with_datadir(mut self, datadir: impl Into<PathBuf>) -> Self {
        self.datadir = datadir.into();
        self
    }

    pub fn with_network(mut self, network: Network) -> Self {
        self.network = network;
        self
    }

    pub fn store_path(&self) -> PathBuf {
        self.datadir.join("store")
    }

    pub fn validate(&self) -> Result<(), NodeError> {
        if self.datadir.as_os_str().is_empty() {
            return Err(NodeError::Config("datadir must not be empty".into()));
        }
        // wire_depth 0 is allowed (epoch-only durability later).
        let _ = (self.wire_depth_blocks, self.archive_durability);
        Ok(())
    }

    pub fn ensure_datadir(&self) -> Result<(), NodeError> {
        self.validate()?;
        std::fs::create_dir_all(&self.datadir).map_err(|source| NodeError::Datadir {
            path: self.datadir.clone(),
            source,
        })?;
        Ok(())
    }
}
