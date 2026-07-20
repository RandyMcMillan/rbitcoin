use crate::error::NodeError;
use rbitcoin_consensus::Milestone;
use rbitcoin_primitives::Network;
use std::net::SocketAddr;
use std::path::PathBuf;

/// Node process configuration (CLI / conf file surface grows over time).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    pub datadir: PathBuf,
    pub network: Network,
    pub archive_durability: bool,
    pub wire_depth_blocks: u32,
    /// Bind address for P2P listen (`None` = do not listen).
    pub p2p_listen: Option<SocketAddr>,
    /// Explicit outbound peers (`--connect`).
    pub connect: Vec<SocketAddr>,
    /// Inject fixed/DNS seeds into addrman when connecting without `--connect`.
    pub use_seeds: bool,
    /// When true, open store and exit (CI / smoke).
    pub smoke: bool,
    /// Cap how long `run_p2p` idles after sync (None = forever). Used by tests.
    pub max_run_secs: Option<u64>,
    /// Electrum TCP listen (`None` = disabled).
    pub electrum_listen: Option<SocketAddr>,
    /// Electrum TLS listen (`None` = disabled). Requires cert + key paths.
    pub electrum_tls_listen: Option<SocketAddr>,
    /// PEM certificate for Electrum TLS.
    pub electrum_tls_cert: Option<PathBuf>,
    /// PEM private key for Electrum TLS.
    pub electrum_tls_key: Option<PathBuf>,
    /// Skip script/prevout checks for blocks at or below this height (0 = off).
    /// Analogous to a coarse assumevalid / milestone for IBD speed.
    pub milestone_height: u32,
    /// How many **live** download peers to keep during IBD / tip follow
    /// (default 16 = `DEFAULT_IBD_TARGET_PEERS`). Seed resolution may use a
    /// larger candidate pool; this is the concurrent live target, not seed count.
    pub max_outbound: u32,
    /// Mempool weight budget in **WU** (default ~300M WU ≈ plan 300 MiB class).
    /// Used for worst-chunk eviction. Override with `--mempool-size-mb`.
    pub mempool_max_weight: u64,
    /// When true, ask systemd (if available) to block automatic suspend/idle
    /// while the node process is running. Off by default.
    pub inhibit_suspend: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            datadir: PathBuf::from("./datadir"),
            network: Network::Mainnet,
            archive_durability: true,
            wire_depth_blocks: 100,
            p2p_listen: None,
            connect: Vec::new(),
            use_seeds: true,
            smoke: false,
            max_run_secs: None,
            electrum_listen: None,
            electrum_tls_listen: None,
            electrum_tls_cert: None,
            electrum_tls_key: None,
            milestone_height: 0,
            // Core-ish outbound budget; IBD redials toward this many live peers.
            max_outbound: 16,
            // ~300e6 weight units — see rbitcoin_mempool::DEFAULT_MAX_MEMPOOL_WEIGHT.
            mempool_max_weight: 300_000_000,
            inhibit_suspend: false,
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

    pub fn with_p2p_listen(mut self, addr: SocketAddr) -> Self {
        self.p2p_listen = Some(addr);
        self
    }

    pub fn store_path(&self) -> PathBuf {
        self.datadir.join("store")
    }

    /// Durable mempool directory (`{datadir}/mempool/`).
    pub fn mempool_path(&self) -> PathBuf {
        self.datadir.join("mempool")
    }

    pub fn milestone(&self) -> Milestone {
        if self.milestone_height == 0 {
            Milestone::NONE
        } else {
            Milestone {
                height: self.milestone_height,
            }
        }
    }

    pub fn validate(&self) -> Result<(), NodeError> {
        if self.datadir.as_os_str().is_empty() {
            return Err(NodeError::Config("datadir must not be empty".into()));
        }
        let _ = (self.wire_depth_blocks, self.archive_durability);
        Ok(())
    }

    /// Create `{datadir}` and standard subdirs (`store`, `mempool`, `wire`) if missing.
    pub fn ensure_datadir(&self) -> Result<(), NodeError> {
        self.validate()?;
        let created_root = !self.datadir.exists();
        std::fs::create_dir_all(&self.datadir).map_err(|source| NodeError::Datadir {
            path: self.datadir.clone(),
            source,
        })?;
        if self.datadir.exists() && !self.datadir.is_dir() {
            return Err(NodeError::Config(format!(
                "datadir is not a directory: {}",
                self.datadir.display()
            )));
        }
        for sub in ["store", "mempool", "wire"] {
            let p = self.datadir.join(sub);
            std::fs::create_dir_all(&p).map_err(|source| NodeError::Datadir {
                path: p,
                source,
            })?;
        }
        if created_root {
            rbitcoin_log::info!(
                "node: created datadir {}",
                self.datadir.display()
            );
        }
        Ok(())
    }
}
