use std::path::PathBuf;
use std::time::Duration;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct Config {
    pub datadir: PathBuf,
    pub rpc_socket: Option<PathBuf>,
    pub rpc_url: Option<String>,
    pub rpc_token_file: Option<PathBuf>,
    pub interval: Duration,
    pub once: bool,
    pub node_binary: Option<PathBuf>,
    pub node_args: Vec<String>,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Self, String> {
        let mut datadir = PathBuf::from(".").join("datadir");
        let mut rpc_socket = None;
        let mut rpc_url = None;
        let mut rpc_token_file = None;
        let mut interval = DEFAULT_INTERVAL;
        let mut once = false;
        let mut node_binary = None;
        let mut node_args = Vec::new();

        let mut iter = args.into_iter();
        let _ = iter.next();
        while let Some(arg) = iter.next() {
            let arg = arg.to_string_lossy().into_owned();
            match arg.as_str() {
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                "-V" | "--version" => {
                    println!("rbitcoin-tui {}", env!("CARGO_PKG_VERSION"));
                    std::process::exit(0);
                }
                "--datadir" => {
                    datadir = PathBuf::from(take_value(&mut iter, "--datadir")?);
                }
                "--rpc-socket" => {
                    rpc_socket = Some(PathBuf::from(take_value(&mut iter, "--rpc-socket")?));
                }
                "--rpc-url" => {
                    rpc_url = Some(take_value(&mut iter, "--rpc-url")?);
                }
                "--rpc-token-file" => {
                    rpc_token_file = Some(PathBuf::from(take_value(&mut iter, "--rpc-token-file")?));
                }
                "--interval" => {
                    interval = Duration::from_secs(parse_u64(
                        &take_value(&mut iter, "--interval")?,
                        "--interval",
                    )?);
                    if interval.is_zero() {
                        return Err("--interval must be at least 1 second".into());
                    }
                }
                "--once" => once = true,
                "--start-node" => {
                    // Legacy no-op. Node startup is now automatic; do not pass
                    // this through to the node binary.
                }
                "--signet" | "--regtest" | "--testnet" | "--mainnet" => {
                    let net = &arg[2..]; // strip leading "--"
                    node_args.push("--network".into());
                    node_args.push(net.into());
                }
                "--node-binary" => {
                    node_binary = Some(PathBuf::from(take_value(&mut iter, "--node-binary")?));
                }
                other => node_args.push(other.to_string()),
            }
        }

        Ok(Self {
            datadir,
            rpc_socket,
            rpc_url,
            rpc_token_file,
            interval,
            once,
            node_binary,
            node_args,
        })
    }

    /// Whether the user explicitly configured an external RPC endpoint.
    pub fn has_external_rpc(&self) -> bool {
        self.rpc_url.is_some() || self.rpc_socket.is_some()
    }
}

const USAGE: &str = "\
rbitcoin-tui [OPTIONS] [NODE_OPTIONS...]

Full-node TUI for rbitcoin. The node is started automatically;
unknown arguments are passed through to the node binary.

TUI Options:
  --datadir PATH        node datadir (default ./datadir)
  --rpc-socket PATH     connect to an existing node unix socket
  --rpc-url URL         connect to an existing node HTTP endpoint
  --rpc-token-file PATH RPC bearer token file for TCP (default PATH/rpc.token)
  --interval SECONDS    refresh interval (default 1)
  --once                print one snapshot and exit
  --node-binary PATH    path to rbitcoin-node binary (default: rbitcoin-node in PATH)
  -h, --help            show this help
  -V, --version         print version

Node Options:
  Any unknown argument is passed through to rbitcoin-node.
  Shorthands: --signet, --regtest, --testnet, --mainnet are translated
  to --network <net> automatically.
  Examples: --network signet, --listen ADDR, --connect ADDR,
  --rpc, --rest, --log-level LEVEL, --prune-seqsigwit, etc.
";

fn take_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> Result<String, String> {
    iter.next()
        .map(|s| s.to_string_lossy().into_owned())
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|e| format!("invalid {flag} `{value}`: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_interval_rejects_zero() {
        let args = vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--interval"),
            std::ffi::OsString::from("0"),
        ];
        assert!(Config::parse(args).is_err());
    }

    #[test]
    fn parse_once_flag() {
        let args = vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--once"),
        ];
        let cfg = Config::parse(args).expect("parse");
        assert!(cfg.once);
    }

    #[test]
    fn parse_node_binary() {
        let args = vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--node-binary"),
            std::ffi::OsString::from("/usr/local/bin/rbitcoin-node"),
        ];
        let cfg = Config::parse(args).expect("parse");
        assert_eq!(
            cfg.node_binary,
            Some(PathBuf::from("/usr/local/bin/rbitcoin-node"))
        );
    }

    #[test]
    fn parse_passes_unknown_args_to_node() {
        let args = vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--listen"),
            std::ffi::OsString::from("0.0.0.0:38333"),
            std::ffi::OsString::from("--rpc"),
        ];
        let cfg = Config::parse(args).expect("parse");
        assert_eq!(cfg.node_args, vec!["--listen", "0.0.0.0:38333", "--rpc"]);
    }

    #[test]
    fn parse_network_shorthands_are_translated() {
        let cfg = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--signet"),
        ])
        .expect("parse");
        assert_eq!(cfg.node_args, vec!["--network", "signet"]);

        let cfg = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--regtest"),
        ])
        .expect("parse");
        assert_eq!(cfg.node_args, vec!["--network", "regtest"]);

        let cfg = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--testnet"),
        ])
        .expect("parse");
        assert_eq!(cfg.node_args, vec!["--network", "testnet"]);

        let cfg = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--mainnet"),
        ])
        .expect("parse");
        assert_eq!(cfg.node_args, vec!["--network", "mainnet"]);
    }

    #[test]
    fn has_external_rpc_detects_explicit_endpoint() {
        let with_url = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--rpc-url"),
            std::ffi::OsString::from("http://127.0.0.1:8332"),
        ])
        .expect("parse");
        assert!(with_url.has_external_rpc());

        let with_socket = Config::parse(vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--rpc-socket"),
            std::ffi::OsString::from("/tmp/rpc.sock"),
        ])
        .expect("parse");
        assert!(with_socket.has_external_rpc());

        let plain = Config::parse(vec![std::ffi::OsString::from("rbitcoin-tui")]).expect("parse");
        assert!(!plain.has_external_rpc());
    }
}
