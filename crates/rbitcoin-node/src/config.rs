use crate::error::NodeError;
use rbitcoin_consensus::Milestone;
use rbitcoin_primitives::Network;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// Default soft densify / archive meter (MiB). Matches historical env default.
pub const DEFAULT_ARCHIVE_QUEUE_MB: u64 = 512;
/// Default max concurrent inbound P2P sessions (Core-ish).
pub const DEFAULT_MAX_INBOUND: u32 = 125;

/// Node process configuration (CLI + optional conf file).
///
/// Operator-critical knobs live here. Advanced IO/perf tunables may still be
/// set via `RBITCOIN_*` env vars (documented as advanced); normal signet/mainnet
/// sync does not require any env export.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    pub datadir: PathBuf,
    pub network: Network,
    pub archive_durability: bool,
    pub wire_depth_blocks: u32,
    /// Bind address for P2P listen (`None` = do not listen / default bind later).
    pub p2p_listen: Option<SocketAddr>,
    /// Explicit outbound peers (`--connect`).
    pub connect: Vec<SocketAddr>,
    /// Inject fixed/DNS seeds into addrman when connecting without `--connect`.
    pub use_seeds: bool,
    /// When true, open store and exit (CI / smoke).
    pub smoke: bool,
    /// Cap how long `run_p2p` idles after sync (None = forever). Used by tests.
    pub max_run_secs: Option<u64>,
    /// Electrum TCP listen (`None` = disabled). Terminate TLS externally if needed.
    pub electrum_listen: Option<SocketAddr>,
    /// Skip script/prevout checks for blocks at or below this height (0 = off).
    /// Analogous to a coarse assumevalid / milestone for IBD speed.
    pub milestone_height: u32,
    /// How many **live** download peers to keep during IBD / tip follow.
    pub max_outbound: u32,
    /// Max concurrent **inbound** P2P sessions (default [`DEFAULT_MAX_INBOUND`]).
    pub max_inbound: u32,
    /// Soft densify / archive meter budget (MiB).
    pub archive_queue_mb: u64,
    /// Optional Class A cache budget (MiB) applied via process env for store code.
    pub class_a_cache_mb: Option<u64>,
    /// Mempool weight budget in **WU** (default ~300M WU ≈ plan 300 MiB class).
    pub mempool_max_weight: u64,
    /// When true, ask systemd (if available) to block automatic suspend/idle.
    pub inhibit_suspend: bool,
    /// Optional conf file path that was loaded (for diagnostics).
    pub conf_path: Option<PathBuf>,
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
            milestone_height: 0,
            max_outbound: 16,
            max_inbound: DEFAULT_MAX_INBOUND,
            archive_queue_mb: DEFAULT_ARCHIVE_QUEUE_MB,
            class_a_cache_mb: None,
            mempool_max_weight: 300_000_000,
            inhibit_suspend: false,
            conf_path: None,
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
        if self.max_outbound == 0 {
            return Err(NodeError::Config("max_outbound must be >= 1".into()));
        }
        if self.max_inbound == 0 {
            return Err(NodeError::Config("max_inbound must be >= 1".into()));
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

    /// Push operator knobs into process env so existing library `from_env` readers
    /// see CLI/conf values without rewriting every hot-path call site.
    ///
    /// Only sets vars for knobs this config owns; does not clear unrelated advanced
    /// envs. Defaults match historical code defaults when CLI omits the flag.
    pub fn apply_operator_env(&self) {
        // Always publish inbound / archive so library code does not need CLI.
        std::env::set_var("RBITCOIN_P2P_MAX_INBOUND", self.max_inbound.to_string());
        std::env::set_var(
            "RBITCOIN_ARCHIVE_QUEUE_MB",
            self.archive_queue_mb.to_string(),
        );
        if let Some(mb) = self.class_a_cache_mb {
            std::env::set_var("RBITCOIN_CLASS_A_CACHE_MB", mb.to_string());
        }
    }

    /// Load a simple `key=value` conf (Core-style lines; `#` comments).
    ///
    /// Supported keys: `datadir`, `network` / `chain`, `listen`, `connect` (repeatable),
    /// `milestone` / `assumevalid_height`, `maxoutbound` / `max_outbound`,
    /// `maxinbound` / `max_inbound` / `maxconnections`, `mempool_size_mb`,
    /// `archive_queue_mb`, `class_a_cache_mb`, `log_level` (informational only here),
    /// `electrum_listen`, `noseeds` / `no_seeds`.
    pub fn merge_conf_file(&mut self, path: &Path) -> Result<(), NodeError> {
        let text = std::fs::read_to_string(path).map_err(|source| NodeError::Config(format!(
            "read conf {}: {source}",
            path.display()
        )))?;
        self.conf_path = Some(path.to_path_buf());
        for (lineno, raw) in text.lines().enumerate() {
            let line = raw.trim();
            if line.is_empty() || line.starts_with('#') || line.starts_with(';') {
                continue;
            }
            let (key, val) = match line.split_once('=') {
                Some((k, v)) => (k.trim(), v.trim()),
                None => {
                    // Boolean Core-style flags: `noseeds=1` preferred; bare `regtest=1`.
                    if line.eq_ignore_ascii_case("regtest") {
                        self.network = Network::Regtest;
                        continue;
                    }
                    if line.eq_ignore_ascii_case("signet") {
                        self.network = Network::Signet;
                        continue;
                    }
                    if line.eq_ignore_ascii_case("testnet") {
                        self.network = Network::Testnet;
                        continue;
                    }
                    return Err(NodeError::Config(format!(
                        "conf {}:{}: expected key=value (got `{line}`)",
                        path.display(),
                        lineno + 1
                    )));
                }
            };
            let key_l = key.to_ascii_lowercase();
            match key_l.as_str() {
                "datadir" => self.datadir = PathBuf::from(val),
                "network" | "chain" => {
                    self.network = Network::parse(val).map_err(|e| {
                        NodeError::Config(format!("conf network: {e}"))
                    })?;
                }
                "listen" => {
                    self.p2p_listen = Some(val.parse().map_err(|e| {
                        NodeError::Config(format!("conf listen: {e}"))
                    })?);
                }
                "connect" => {
                    self.connect.push(val.parse().map_err(|e| {
                        NodeError::Config(format!("conf connect: {e}"))
                    })?);
                }
                "electrum_listen" | "electrumlisten" => {
                    self.electrum_listen = Some(val.parse().map_err(|e| {
                        NodeError::Config(format!("conf electrum_listen: {e}"))
                    })?);
                }
                "milestone" | "assumevalid_height" | "assumevalidheight" => {
                    self.milestone_height = val.parse().map_err(|e| {
                        NodeError::Config(format!("conf milestone: {e}"))
                    })?;
                }
                "maxoutbound" | "max_outbound" => {
                    self.max_outbound = val.parse().map_err(|e| {
                        NodeError::Config(format!("conf maxoutbound: {e}"))
                    })?;
                }
                "maxinbound" | "max_inbound" | "maxconnections" => {
                    self.max_inbound = val.parse().map_err(|e| {
                        NodeError::Config(format!("conf maxinbound: {e}"))
                    })?;
                }
                "mempool_size_mb" | "maxmempool" => {
                    let mb: u64 = val.parse().map_err(|e| {
                        NodeError::Config(format!("conf mempool_size_mb: {e}"))
                    })?;
                    if mb == 0 {
                        return Err(NodeError::Config(
                            "conf mempool_size_mb must be >= 1".into(),
                        ));
                    }
                    self.mempool_max_weight = mb.saturating_mul(1_000_000);
                }
                "archive_queue_mb" => {
                    self.archive_queue_mb = val.parse().map_err(|e| {
                        NodeError::Config(format!("conf archive_queue_mb: {e}"))
                    })?;
                }
                "class_a_cache_mb" => {
                    self.class_a_cache_mb = Some(val.parse().map_err(|e| {
                        NodeError::Config(format!("conf class_a_cache_mb: {e}"))
                    })?);
                }
                "noseeds" | "no_seeds" => {
                    self.use_seeds = !is_conf_true(val);
                }
                "log_level" | "debug" => {
                    // Applied by CLI after merge; ignore here if bare conf load.
                }
                "regtest" if is_conf_true(val) => self.network = Network::Regtest,
                "signet" if is_conf_true(val) => self.network = Network::Signet,
                "testnet" if is_conf_true(val) => self.network = Network::Testnet,
                other => {
                    rbitcoin_log::warn!(
                        "node: conf {}:{}: unknown key `{other}` ignored",
                        path.display(),
                        lineno + 1
                    );
                }
            }
        }
        Ok(())
    }
}

fn is_conf_true(val: &str) -> bool {
    matches!(
        val.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | ""
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("rbitcoin-node-cfg-{n}"))
    }

    #[test]
    fn builders_paths_milestone_and_ensure() {
        let dir = tmp();
        let cfg = NodeConfig::default()
            .with_datadir(&dir)
            .with_network(Network::Regtest)
            .with_p2p_listen("127.0.0.1:0".parse().unwrap());
        assert_eq!(cfg.network, Network::Regtest);
        assert_eq!(cfg.store_path(), dir.join("store"));
        assert_eq!(cfg.mempool_path(), dir.join("mempool"));
        assert_eq!(cfg.max_inbound, DEFAULT_MAX_INBOUND);
        assert_eq!(cfg.archive_queue_mb, DEFAULT_ARCHIVE_QUEUE_MB);
        cfg.ensure_datadir().unwrap();
        assert!(dir.join("store").is_dir());
        assert!(dir.join("mempool").is_dir());
        cfg.ensure_datadir().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn conf_file_maps_operator_knobs() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("rbitcoin.conf");
        std::fs::write(
            &conf,
            "# test conf\n\
             network=signet\n\
             maxinbound=40\n\
             maxoutbound=8\n\
             archive_queue_mb=128\n\
             mempool_size_mb=50\n\
             milestone=100\n\
             connect=127.0.0.1:38333\n",
        )
        .unwrap();
        let mut cfg = NodeConfig::default().with_datadir(dir.join("data"));
        cfg.merge_conf_file(&conf).unwrap();
        assert_eq!(cfg.network, Network::Signet);
        assert_eq!(cfg.max_inbound, 40);
        assert_eq!(cfg.max_outbound, 8);
        assert_eq!(cfg.archive_queue_mb, 128);
        assert_eq!(cfg.mempool_max_weight, 50_000_000);
        assert_eq!(cfg.milestone_height, 100);
        assert_eq!(cfg.connect.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn operator_knob_defaults_and_fields() {
        // Do not call apply_operator_env here — process env is shared with CLI tests.
        let cfg = NodeConfig {
            max_inbound: 42,
            archive_queue_mb: 64,
            class_a_cache_mb: Some(128),
            ..NodeConfig::default()
        };
        assert_eq!(cfg.max_inbound, 42);
        assert_eq!(cfg.archive_queue_mb, 64);
        assert_eq!(cfg.class_a_cache_mb, Some(128));
        assert_eq!(NodeConfig::default().max_inbound, DEFAULT_MAX_INBOUND);
        assert_eq!(NodeConfig::default().archive_queue_mb, DEFAULT_ARCHIVE_QUEUE_MB);
    }

    #[test]
    fn ensure_datadir_rejects_file_path_after_parent_exists() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let file_as_dir = dir.join("notadir");
        std::fs::write(&file_as_dir, b"x").unwrap();
        let cfg = NodeConfig::default().with_datadir(&file_as_dir);
        assert!(cfg.ensure_datadir().is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ensure_datadir_rejects_file_as_subdir() {
        let dir = tmp();
        let cfg = NodeConfig::default().with_datadir(&dir);
        cfg.ensure_datadir().unwrap();
        // Make store a file so recreate fails.
        let _ = std::fs::remove_dir_all(dir.join("store"));
        std::fs::write(dir.join("store"), b"x").unwrap();
        let err = cfg.ensure_datadir().unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("store") || msg.contains("datadir") || msg.contains("File exists"),
            "{msg}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
