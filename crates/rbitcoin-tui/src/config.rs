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
    pub start_node: bool,
    pub node_binary: Option<PathBuf>,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Self, String> {
        let mut datadir = PathBuf::from(".").join("datadir");
        let mut rpc_socket = None;
        let mut rpc_url = None;
        let mut rpc_token_file = None;
        let mut interval = DEFAULT_INTERVAL;
        let mut once = false;
        let mut start_node = false;
        let mut node_binary = None;

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
                "--start-node" => start_node = true,
                "--node-binary" => {
                    node_binary = Some(PathBuf::from(take_value(&mut iter, "--node-binary")?));
                }
                other if other.starts_with('-') => return Err(format!("unknown flag `{other}`")),
                other => return Err(format!("unexpected argument `{other}`")),
            }
        }

        Ok(Self {
            datadir,
            rpc_socket,
            rpc_url,
            rpc_token_file,
            interval,
            once,
            start_node,
            node_binary,
        })
    }
}

const USAGE: &str = "\
rbitcoin-tui [OPTIONS]

TUI client for a running rbitcoin node JSON-RPC endpoint.

Options:
  --datadir PATH        node datadir (default ./datadir); local socket is PATH/rpc.sock
  --rpc-socket PATH     unix socket the node binds with --rpc-socket
  --rpc-url URL         HTTP RPC endpoint (for example http://127.0.0.1:8332)
  --rpc-token-file PATH RPC bearer token file for TCP (default PATH/rpc.token)
  --interval SECONDS    refresh interval (default 1)
  --once                print one snapshot and exit
  --start-node          start rbitcoin-node if no RPC endpoint responds
  --node-binary PATH    path to rbitcoin-node binary (default: rbitcoin-node in PATH)
  -h, --help            show this help
  -V, --version         print version
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
    fn parse_start_node_flag() {
        let args = vec![
            std::ffi::OsString::from("rbitcoin-tui"),
            std::ffi::OsString::from("--start-node"),
        ];
        let cfg = Config::parse(args).expect("parse");
        assert!(cfg.start_node);
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
}
