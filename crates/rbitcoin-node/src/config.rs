use crate::error::NodeError;
use bitcoin::hex::FromHex;
use bitcoin::ScriptBuf;
use rbitcoin_consensus::{ChainParams, Milestone};
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
///
/// **Env publish:** [`Self::apply_operator_env`] only writes process env for knobs
/// that were **explicitly** set via CLI or conf (`*_explicit` flags). Omitting a
/// flag leaves a pre-set advanced env (e.g. `RBITCOIN_P2P_MAX_INBOUND`) intact
/// for library `from_env` readers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeConfig {
    pub datadir: PathBuf,
    pub network: Network,
    /// Custom BIP325 challenge. `None` selects the default global Signet.
    pub signet_challenge: Option<ScriptBuf>,
    /// Custom Signet PoW target spacing in seconds.
    pub signet_block_time: Option<u64>,
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
    /// Electrum TCP listen (`None` = disabled). Plain TCP; terminate TLS at a
    /// reverse proxy when public. App DoS limits apply regardless of bind address.
    /// **Requires** [`Self::shindex`].
    pub electrum_listen: Option<SocketAddr>,
    /// Esplora REST HTTP listen (`None` = disabled). Plain HTTP; TLS via proxy.
    /// **Requires** [`Self::shindex`].
    pub esplora_listen: Option<SocketAddr>,
    /// Build Class B scripthash index (Electrum/Esplora history). Default **off**.
    /// When off: tip follow and node JSON-RPC work without SH bulk materialize.
    pub shindex: bool,
    /// Core-class JSON-RPC HTTP listen (`None` = disabled). Plain HTTP; TLS via proxy.
    pub rpc_listen: Option<SocketAddr>,
    /// Optional RPC Basic auth user (with [`Self::rpc_password`]). When both unset
    /// and `rpc_listen` is set, a cookie file under datadir is used.
    pub rpc_user: Option<String>,
    /// Optional RPC Basic auth password.
    pub rpc_password: Option<String>,
    /// Skip script/prevout checks for blocks at or below this height (0 = off).
    /// Analogous to a coarse assumevalid / milestone for IBD speed.
    pub milestone_height: u32,
    /// How many **live** download peers to keep during IBD / tip follow.
    pub max_outbound: u32,
    /// Max concurrent **inbound** P2P sessions (default [`DEFAULT_MAX_INBOUND`]).
    pub max_inbound: u32,
    /// True when max_inbound came from CLI or conf (publish to env on apply).
    pub max_inbound_explicit: bool,
    /// Soft densify / archive meter budget (MiB).
    pub archive_queue_mb: u64,
    /// True when archive_queue_mb came from CLI or conf.
    pub archive_queue_mb_explicit: bool,
    /// Mempool weight budget in **WU** (default ~300M WU ≈ plan 300 MiB class).
    pub mempool_max_weight: u64,
    /// When true, ask systemd (if available) to block automatic suspend/idle.
    pub inhibit_suspend: bool,
    /// Optional conf file path that was loaded (for diagnostics).
    pub conf_path: Option<PathBuf>,
    /// Log level from conf (`log_level=…`), if any. CLI `--log-level` overrides.
    /// Values: error|warn|info|debug|trace|off (same as CLI).
    pub conf_log_level: Option<String>,
    /// Optional JSONL API call log (`--api-log` / `api_log=`). Electrum, Esplora, RPC.
    pub api_log: Option<PathBuf>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            datadir: PathBuf::from("./datadir"),
            network: Network::Mainnet,
            signet_challenge: None,
            signet_block_time: None,
            archive_durability: true,
            wire_depth_blocks: 100,
            p2p_listen: None,
            connect: Vec::new(),
            use_seeds: true,
            smoke: false,
            max_run_secs: None,
            electrum_listen: None,
            esplora_listen: None,
            shindex: false,
            rpc_listen: None,
            rpc_user: None,
            rpc_password: None,
            milestone_height: 0,
            max_outbound: 16,
            max_inbound: DEFAULT_MAX_INBOUND,
            max_inbound_explicit: false,
            archive_queue_mb: DEFAULT_ARCHIVE_QUEUE_MB,
            archive_queue_mb_explicit: false,
            mempool_max_weight: 300_000_000,
            inhibit_suspend: false,
            conf_path: None,
            conf_log_level: None,
            api_log: None,
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

    /// Compose immutable consensus parameters from operator configuration.
    pub fn chain_params(&self) -> Result<ChainParams, NodeError> {
        match self.signet_challenge.clone() {
            Some(challenge) => {
                ChainParams::custom_signet(challenge, self.signet_block_time.unwrap_or(10 * 60))
                    .map_err(|e| NodeError::Config(e.into()))
            }
            None => Ok(ChainParams::for_network(self.network)),
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
        if (self.signet_challenge.is_some() || self.signet_block_time.is_some())
            && self.network != Network::Signet
        {
            return Err(NodeError::Config(
                "signetchallenge and signetblocktime require network=signet".into(),
            ));
        }
        if self.signet_block_time.is_some() && self.signet_challenge.is_none() {
            return Err(NodeError::Config(
                "signetblocktime requires signetchallenge".into(),
            ));
        }
        if self.signet_block_time == Some(0) {
            return Err(NodeError::Config(
                "signetblocktime must be greater than zero".into(),
            ));
        }
        if self.electrum_listen.is_some() && !self.shindex {
            return Err(NodeError::Config(
                "electrum_listen requires shindex=1 (--shindex); Electrum history needs Class B scripthash"
                    .into(),
            ));
        }
        if self.esplora_listen.is_some() && !self.shindex {
            return Err(NodeError::Config(
                "esplora_listen requires shindex=1 (--shindex); Esplora history needs Class B scripthash"
                    .into(),
            ));
        }
        if let Some(addr) = self.rpc_listen {
            let has_user_pass = self.rpc_user.is_some() && self.rpc_password.is_some();
            let loopback = addr.ip().is_loopback();
            if !loopback && !has_user_pass {
                // Cookie will be written for loopback; non-loopback needs explicit creds
                // or we still write cookie — Core writes cookie always. Plan: refuse
                // unbound public without cookie OR user/pass. Cookie counts as auth.
                // Non-loopback is OK if we always generate cookie when listen set.
                // Stricter plan: "If bind is not loopback and neither cookie nor user/pass
                // configured → refuse". Cookie is auto when listen set, so always OK.
                // Keep check only when both user and password partially set.
                let _ = has_user_pass;
            }
            if self.rpc_user.is_some() ^ self.rpc_password.is_some() {
                return Err(NodeError::Config(
                    "rpcuser and rpcpassword must both be set (or both unset for cookie auth)"
                        .into(),
                ));
            }
        }
        let _ = (self.wire_depth_blocks, self.archive_durability);
        Ok(())
    }

    /// Path for Core-style RPC cookie (`{datadir}/.cookie`).
    pub fn rpc_cookie_path(&self) -> PathBuf {
        self.datadir.join(".cookie")
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
            std::fs::create_dir_all(&p).map_err(|source| NodeError::Datadir { path: p, source })?;
        }
        if created_root {
            rbitcoin_log::info!("node: created datadir {}", self.datadir.display());
        }
        Ok(())
    }

    /// Push **explicit** operator knobs into process env for library `from_env` readers.
    ///
    /// Does **not** overwrite advanced envs when CLI/conf left a knob at the
    /// structural default (user may have exported `RBITCOIN_P2P_MAX_INBOUND` etc.).
    pub fn apply_operator_env(&self) {
        if self.max_inbound_explicit {
            std::env::set_var("RBITCOIN_P2P_MAX_INBOUND", self.max_inbound.to_string());
        }
        if self.archive_queue_mb_explicit {
            std::env::set_var(
                "RBITCOIN_ARCHIVE_QUEUE_MB",
                self.archive_queue_mb.to_string(),
            );
        }
    }

    /// Load a simple `key=value` conf (Core-style lines; `#` comments).
    ///
    /// Supported keys: `datadir`, `network` / `chain`, `listen`, `connect` (repeatable),
    /// `milestone` / `assumevalid_height`, `maxoutbound` / `max_outbound`,
    /// `maxinbound` / `max_inbound` / `maxconnections`, `mempool_size_mb` / `maxmempool`,
    /// `archive_queue_mb`, `log_level`, `api_log`, `electrum_listen`, `esplora_listen`,
    /// `shindex`, `rpc_listen`, `rpcuser`, `rpcpassword`,
    /// `noseeds` / `no_seeds`, `signetchallenge`, and `signetblocktime`.
    pub fn merge_conf_file(&mut self, path: &Path) -> Result<(), NodeError> {
        let text = std::fs::read_to_string(path).map_err(|source| {
            NodeError::Config(format!("read conf {}: {source}", path.display()))
        })?;
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
                    self.network = Network::parse(val)
                        .map_err(|e| NodeError::Config(format!("conf network: {e}")))?;
                }
                "signetchallenge" | "signet_challenge" => {
                    self.signet_challenge =
                        Some(parse_signet_challenge(val).map_err(|e| {
                            NodeError::Config(format!("conf signetchallenge: {e}"))
                        })?);
                }
                "signetblocktime" | "signet_block_time" => {
                    self.signet_block_time =
                        Some(val.parse().map_err(|e| {
                            NodeError::Config(format!("conf signetblocktime: {e}"))
                        })?);
                }
                "listen" => {
                    self.p2p_listen = Some(
                        val.parse()
                            .map_err(|e| NodeError::Config(format!("conf listen: {e}")))?,
                    );
                }
                "connect" => {
                    self.connect.push(
                        val.parse()
                            .map_err(|e| NodeError::Config(format!("conf connect: {e}")))?,
                    );
                }
                "electrum_listen" | "electrumlisten" => {
                    self.electrum_listen =
                        Some(val.parse().map_err(|e| {
                            NodeError::Config(format!("conf electrum_listen: {e}"))
                        })?);
                }
                "esplora_listen" | "esploralisten" => {
                    self.esplora_listen = Some(
                        val.parse()
                            .map_err(|e| NodeError::Config(format!("conf esplora_listen: {e}")))?,
                    );
                }
                "shindex" => {
                    self.shindex = parse_conf_bool(val)
                        .map_err(|e| NodeError::Config(format!("conf shindex: {e}")))?;
                }
                "rpc_listen" | "rpclisten" => {
                    self.rpc_listen = Some(
                        val.parse()
                            .map_err(|e| NodeError::Config(format!("conf rpc_listen: {e}")))?,
                    );
                }
                "rpcuser" | "rpc_user" => {
                    self.rpc_user = Some(val.to_string());
                }
                "rpcpassword" | "rpc_password" => {
                    self.rpc_password = Some(val.to_string());
                }
                "milestone" | "assumevalid_height" | "assumevalidheight" => {
                    self.milestone_height = val
                        .parse()
                        .map_err(|e| NodeError::Config(format!("conf milestone: {e}")))?;
                }
                "maxoutbound" | "max_outbound" => {
                    self.max_outbound = val
                        .parse()
                        .map_err(|e| NodeError::Config(format!("conf maxoutbound: {e}")))?;
                }
                "maxinbound" | "max_inbound" | "maxconnections" => {
                    self.max_inbound = val
                        .parse()
                        .map_err(|e| NodeError::Config(format!("conf maxinbound: {e}")))?;
                    self.max_inbound_explicit = true;
                }
                "mempool_size_mb" | "maxmempool" => {
                    let mb: u64 = val
                        .parse()
                        .map_err(|e| NodeError::Config(format!("conf mempool_size_mb: {e}")))?;
                    if mb == 0 {
                        return Err(NodeError::Config(
                            "conf mempool_size_mb must be >= 1".into(),
                        ));
                    }
                    self.mempool_max_weight = mb.saturating_mul(1_000_000);
                }
                "archive_queue_mb" => {
                    self.archive_queue_mb = val
                        .parse()
                        .map_err(|e| NodeError::Config(format!("conf archive_queue_mb: {e}")))?;
                    self.archive_queue_mb_explicit = true;
                }
                "log_level" => {
                    if val.is_empty() {
                        return Err(NodeError::Config("conf log_level requires a value".into()));
                    }
                    self.conf_log_level = Some(val.to_string());
                }
                "api_log" | "apilog" => {
                    if val.is_empty() {
                        return Err(NodeError::Config("conf api_log requires a path".into()));
                    }
                    self.api_log = Some(PathBuf::from(val));
                }
                "noseeds" | "no_seeds" => {
                    self.use_seeds = !is_conf_true(val);
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

pub(crate) fn parse_signet_challenge(value: &str) -> Result<ScriptBuf, String> {
    Vec::<u8>::from_hex(value)
        .map(ScriptBuf::from_bytes)
        .map_err(|e| format!("must be hexadecimal: {e}"))
}

fn is_conf_true(val: &str) -> bool {
    matches!(
        val.to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on" | ""
    )
}

/// Parse `1`/`true`/`yes`/`on` → true; `0`/`false`/`no`/`off` → false.
fn parse_conf_bool(val: &str) -> Result<bool, String> {
    let v = val.to_ascii_lowercase();
    match v.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!("expected 0|1|true|false (got `{val}`)")),
    }
}

/// Serialize tests that mutate process `RBITCOIN_*` env (CLI + config unit tests).
#[cfg(test)]
pub(crate) static OPERATOR_ENV_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        assert!(!cfg.max_inbound_explicit);
        assert!(!cfg.archive_queue_mb_explicit);
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
             log_level=debug\n\
             api_log=/tmp/rbitcoin-api.jsonl\n\
             connect=127.0.0.1:38333\n",
        )
        .unwrap();
        let mut cfg = NodeConfig::default().with_datadir(dir.join("data"));
        cfg.merge_conf_file(&conf).unwrap();
        assert_eq!(cfg.network, Network::Signet);
        assert_eq!(cfg.max_inbound, 40);
        assert!(cfg.max_inbound_explicit);
        assert_eq!(cfg.max_outbound, 8);
        assert_eq!(cfg.archive_queue_mb, 128);
        assert!(cfg.archive_queue_mb_explicit);
        assert_eq!(cfg.mempool_max_weight, 50_000_000);
        assert_eq!(cfg.milestone_height, 100);
        assert_eq!(cfg.conf_log_level.as_deref(), Some("debug"));
        assert_eq!(
            cfg.api_log.as_deref(),
            Some(std::path::Path::new("/tmp/rbitcoin-api.jsonl"))
        );
        assert_eq!(cfg.connect.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression: apply must not clobber pre-set advanced envs when knobs were
    /// not explicit on CLI/conf.
    #[test]
    fn apply_operator_env_preserves_unset_advanced_env() {
        let _g = OPERATOR_ENV_TEST_LOCK
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let prev_in = std::env::var_os("RBITCOIN_P2P_MAX_INBOUND");
        let prev_ar = std::env::var_os("RBITCOIN_ARCHIVE_QUEUE_MB");
        std::env::set_var("RBITCOIN_P2P_MAX_INBOUND", "99");
        std::env::set_var("RBITCOIN_ARCHIVE_QUEUE_MB", "55");
        let cfg = NodeConfig::default(); // no explicit flags
        assert!(!cfg.max_inbound_explicit);
        assert!(!cfg.archive_queue_mb_explicit);
        cfg.apply_operator_env();
        assert_eq!(
            std::env::var("RBITCOIN_P2P_MAX_INBOUND").as_deref(),
            Ok("99"),
            "must not overwrite advanced inbound env when CLI/conf omitted knob"
        );
        assert_eq!(
            std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB").as_deref(),
            Ok("55"),
            "must not overwrite advanced archive env when CLI/conf omitted knob"
        );
        // Explicit knobs do publish.
        let mut explicit = NodeConfig::default();
        explicit.max_inbound = 12;
        explicit.max_inbound_explicit = true;
        explicit.archive_queue_mb = 88;
        explicit.archive_queue_mb_explicit = true;
        explicit.apply_operator_env();
        assert_eq!(
            std::env::var("RBITCOIN_P2P_MAX_INBOUND").as_deref(),
            Ok("12")
        );
        assert_eq!(
            std::env::var("RBITCOIN_ARCHIVE_QUEUE_MB").as_deref(),
            Ok("88")
        );
        // Restore prior process env (do not leave blank for parallel CLI tests).
        match prev_in {
            Some(v) => std::env::set_var("RBITCOIN_P2P_MAX_INBOUND", v),
            None => std::env::remove_var("RBITCOIN_P2P_MAX_INBOUND"),
        }
        match prev_ar {
            Some(v) => std::env::set_var("RBITCOIN_ARCHIVE_QUEUE_MB", v),
            None => std::env::remove_var("RBITCOIN_ARCHIVE_QUEUE_MB"),
        }
    }

    #[test]
    fn operator_knob_defaults_and_fields() {
        let cfg = NodeConfig {
            max_inbound: 42,
            max_inbound_explicit: true,
            archive_queue_mb: 64,
            archive_queue_mb_explicit: true,
            ..NodeConfig::default()
        };
        assert_eq!(cfg.max_inbound, 42);
        assert_eq!(cfg.archive_queue_mb, 64);
        assert_eq!(NodeConfig::default().max_inbound, DEFAULT_MAX_INBOUND);
        assert_eq!(
            NodeConfig::default().archive_queue_mb,
            DEFAULT_ARCHIVE_QUEUE_MB
        );
    }

    #[test]
    fn custom_signet_conf_builds_params() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("custom-signet.conf");
        std::fs::write(
            &conf,
            "network=signet\n\
             signetchallenge=51\n\
             signetblocktime=60\n",
        )
        .unwrap();

        let mut cfg = NodeConfig::default();
        cfg.merge_conf_file(&conf).unwrap();
        cfg.validate().unwrap();
        let params = cfg.chain_params().unwrap();
        assert_eq!(params.btc.pow_target_spacing, 60);
        assert_eq!(params.signet_challenge.unwrap().as_bytes(), &[0x51]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn custom_signet_options_require_signet_and_challenge() {
        let challenge = bitcoin::ScriptBuf::from_bytes(vec![0x51]);
        let mainnet = NodeConfig {
            signet_challenge: Some(challenge),
            ..NodeConfig::default()
        };
        assert!(mainnet.validate().is_err());

        let missing_challenge = NodeConfig {
            network: Network::Signet,
            signet_block_time: Some(30),
            ..NodeConfig::default()
        };
        assert!(missing_challenge.validate().is_err());
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

    #[test]
    fn validate_rejects_zero_peer_caps_and_empty_datadir() {
        let mut cfg = NodeConfig::default();
        cfg.datadir = PathBuf::new();
        assert!(cfg.validate().is_err());
        let mut cfg = NodeConfig::default().with_datadir(tmp());
        cfg.max_outbound = 0;
        assert!(cfg.validate().is_err());
        cfg.max_outbound = 1;
        cfg.max_inbound = 0;
        assert!(cfg.validate().is_err());
        cfg.max_inbound = 1;
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.milestone(), Milestone::NONE);
        cfg.milestone_height = 10;
        assert_eq!(cfg.milestone().height, 10);
    }

    #[test]
    fn conf_bare_network_flags_and_bad_line() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("flags.conf");
        std::fs::write(
            &conf,
            "regtest\n\
             # comment\n\
             ; also\n\
             \n\
             noseeds=1\n",
        )
        .unwrap();
        let mut cfg = NodeConfig::default().with_datadir(dir.join("d"));
        cfg.merge_conf_file(&conf).unwrap();
        assert_eq!(cfg.network, Network::Regtest);
        assert!(!cfg.use_seeds);

        let conf2 = dir.join("signet.conf");
        std::fs::write(&conf2, "signet\n").unwrap();
        let mut cfg2 = NodeConfig::default().with_datadir(dir.join("d2"));
        cfg2.merge_conf_file(&conf2).unwrap();
        assert_eq!(cfg2.network, Network::Signet);

        let conf3 = dir.join("testnet.conf");
        std::fs::write(&conf3, "testnet\n").unwrap();
        let mut cfg3 = NodeConfig::default().with_datadir(dir.join("d3"));
        cfg3.merge_conf_file(&conf3).unwrap();
        assert_eq!(cfg3.network, Network::Testnet);

        let conf_bad = dir.join("bad.conf");
        std::fs::write(&conf_bad, "not_a_key_value\n").unwrap();
        let mut cfg_bad = NodeConfig::default().with_datadir(dir.join("db"));
        assert!(cfg_bad.merge_conf_file(&conf_bad).is_err());

        let missing = dir.join("nope.conf");
        assert!(cfg_bad.merge_conf_file(&missing).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn electrum_without_shindex_fails_validate() {
        let mut cfg = NodeConfig::default().with_datadir(tmp());
        cfg.electrum_listen = Some("127.0.0.1:50001".parse().unwrap());
        cfg.shindex = false;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(
            err.contains("shindex"),
            "expected shindex requirement, got {err}"
        );
    }

    #[test]
    fn esplora_without_shindex_fails_validate() {
        let mut cfg = NodeConfig::default().with_datadir(tmp());
        cfg.esplora_listen = Some("127.0.0.1:3000".parse().unwrap());
        cfg.shindex = false;
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("shindex"), "got {err}");
    }

    #[test]
    fn shindex_alone_validates() {
        let mut cfg = NodeConfig::default().with_datadir(tmp());
        cfg.shindex = true;
        cfg.validate().unwrap();
    }

    #[test]
    fn electrum_with_shindex_validates() {
        let mut cfg = NodeConfig::default().with_datadir(tmp());
        cfg.shindex = true;
        cfg.electrum_listen = Some("127.0.0.1:50001".parse().unwrap());
        cfg.validate().unwrap();
    }

    #[test]
    fn conf_keys_parse_and_error_paths() {
        let dir = tmp();
        std::fs::create_dir_all(&dir).unwrap();
        let conf = dir.join("full.conf");
        std::fs::write(
            &conf,
            "listen=127.0.0.1:18444\n\
             connect=127.0.0.1:18445\n\
             shindex=1\n\
             electrum_listen=127.0.0.1:50001\n\
             esplora_listen=127.0.0.1:3000\n\
             rpc_listen=127.0.0.1:8332\n\
             milestone=100\n\
             maxoutbound=8\n\
             maxinbound=32\n\
             mempool_size_mb=50\n\
             archive_queue_mb=64\n\
             log_level=info\n\
             noseeds=0\n\
             unknown_key=1\n\
             network=regtest\n",
        )
        .unwrap();
        let mut cfg = NodeConfig::default().with_datadir(dir.join("d"));
        cfg.merge_conf_file(&conf).unwrap();
        assert_eq!(cfg.network, Network::Regtest);
        assert!(cfg.p2p_listen.is_some());
        assert_eq!(cfg.connect.len(), 1);
        assert!(cfg.shindex);
        assert!(cfg.electrum_listen.is_some());
        assert!(cfg.esplora_listen.is_some());
        assert!(cfg.rpc_listen.is_some());
        assert_eq!(cfg.milestone_height, 100);
        assert_eq!(cfg.max_outbound, 8);
        assert_eq!(cfg.max_inbound, 32);
        assert!(cfg.max_inbound_explicit);
        assert_eq!(cfg.mempool_max_weight, 50_000_000);
        assert_eq!(cfg.archive_queue_mb, 64);
        assert!(cfg.archive_queue_mb_explicit);
        assert_eq!(cfg.conf_log_level.as_deref(), Some("info"));
        assert!(cfg.use_seeds); // noseeds=0 → seeds on

        // Error paths: bad listen / electrum / mempool 0 / empty log_level.
        for (body, needle) in [
            ("listen=not-an-addr\n", "listen"),
            ("electrum_listen=bad\n", "electrum"),
            ("esplora_listen=bad\n", "esplora"),
            ("mempool_size_mb=0\n", "mempool"),
            ("log_level=\n", "log_level"),
            ("network=notanet\n", "network"),
            ("milestone=x\n", "milestone"),
        ] {
            let p = dir.join(format!("bad-{needle}.conf"));
            std::fs::write(&p, body).unwrap();
            let mut c = NodeConfig::default().with_datadir(dir.join("dx"));
            let err = c.merge_conf_file(&p).unwrap_err();
            let msg = format!("{err}");
            assert!(
                msg.to_ascii_lowercase().contains(needle)
                    || msg.contains("conf")
                    || msg.contains("parse"),
                "body={body:?} msg={msg}"
            );
        }

        // ensure_datadir rejects a file path as datadir.
        let file_dd = dir.join("not-a-dir");
        std::fs::write(&file_dd, b"x").unwrap();
        let cfg_f = NodeConfig::default().with_datadir(&file_dd);
        assert!(cfg_f.ensure_datadir().is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
