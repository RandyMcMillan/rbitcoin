use crate::config::Config;
use crate::rpc::{
    parse_http_url, BlockchainInfo, MempoolInfo, NetworkInfo, PeerInfo, RpcEndpoint,
};
use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const HISTORY_CAPACITY: usize = 120;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tab {
    Dashboard,
    Chain,
    Network,
    Mempool,
    Console,
}

impl Tab {
    pub const ALL: &[Tab] = &[
        Tab::Dashboard,
        Tab::Chain,
        Tab::Network,
        Tab::Mempool,
        Tab::Console,
    ];

    pub fn title(self) -> &'static str {
        match self {
            Tab::Dashboard => "Dashboard",
            Tab::Chain => "Chain",
            Tab::Network => "Network",
            Tab::Mempool => "Mempool",
            Tab::Console => "Console",
        }
    }

    pub fn next(self) -> Tab {
        match self {
            Tab::Dashboard => Tab::Chain,
            Tab::Chain => Tab::Network,
            Tab::Network => Tab::Mempool,
            Tab::Mempool => Tab::Console,
            Tab::Console => Tab::Dashboard,
        }
    }

    pub fn prev(self) -> Tab {
        match self {
            Tab::Dashboard => Tab::Console,
            Tab::Chain => Tab::Dashboard,
            Tab::Network => Tab::Chain,
            Tab::Mempool => Tab::Network,
            Tab::Console => Tab::Mempool,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Snapshot {
    pub endpoint: RpcEndpoint,
    pub refreshed_at: Option<Instant>,
    pub chain: Result<BlockchainInfo, String>,
    pub network: Result<NetworkInfo, String>,
    pub mempool: Result<MempoolInfo, String>,
    pub peers: Result<Vec<PeerInfo>, String>,
    pub warnings: Option<String>,
    pub last_error: Option<String>,
}

impl Snapshot {
    pub fn empty() -> Self {
        Self {
            endpoint: RpcEndpoint::Unix(PathBuf::from(".")),
            refreshed_at: None,
            chain: Err("waiting".into()),
            network: Err("waiting".into()),
            mempool: Err("waiting".into()),
            peers: Err("waiting".into()),
            warnings: None,
            last_error: None,
        }
    }

    pub fn fetch(config: &Config, timeout: Duration) -> Self {
        let mut endpoint = None;
        let refreshed_at = Some(Instant::now());
        let mut chain = Err("no RPC endpoint responded".to_string());
        let mut network = Err("no RPC endpoint responded".to_string());
        let mut mempool = Err("no RPC endpoint responded".to_string());
        let mut peers = Err("no RPC endpoint responded".to_string());
        let mut errors = Vec::new();

        for candidate in config.endpoints() {
            let c = crate::rpc::rpc_call(&candidate, "getblockchaininfo", &[], timeout)
                .and_then(BlockchainInfo::from_value);
            let n = crate::rpc::rpc_call(&candidate, "getnetworkinfo", &[], timeout)
                .and_then(NetworkInfo::from_value);
            let m = crate::rpc::rpc_call(&candidate, "getmempoolinfo", &[], timeout)
                .and_then(MempoolInfo::from_value);
            let p = crate::rpc::rpc_call(&candidate, "getpeerinfo", &[], timeout)
                .and_then(|v| match v {
                    serde_json::Value::Array(arr) => arr
                        .into_iter()
                        .map(PeerInfo::from_value)
                        .collect::<Result<Vec<_>, _>>(),
                    other => Err(format!("expected array, got {other}")),
                });

            // Accept the endpoint as soon as chain info works; keep whatever
            // else succeeded. This prevents a slow mempool or peer call from
            // blanking the whole UI during sync.
            if c.is_ok() {
                endpoint = Some(candidate);
                chain = c;
                network = n;
                mempool = m;
                peers = p;
                break;
            }

            errors.push(format!(
                "{}: {} | {} | {}",
                candidate.label(),
                c.as_ref().err().cloned().unwrap_or_else(|| "ok".into()),
                n.as_ref().err().cloned().unwrap_or_else(|| "ok".into()),
                m.as_ref().err().cloned().unwrap_or_else(|| "ok".into())
            ));
        }
        let endpoint =
            endpoint.unwrap_or_else(|| RpcEndpoint::Unix(PathBuf::from("./datadir/rpc.sock")));
        let warnings = chain
            .as_ref()
            .ok()
            .and_then(|c| c.warnings.clone())
            .or_else(|| network.as_ref().ok().and_then(|n| n.warnings.clone()));
        let last_error = chain
            .as_ref()
            .err()
            .cloned()
            .or_else(|| network.as_ref().err().cloned())
            .or_else(|| mempool.as_ref().err().cloned())
            .or_else(|| peers.as_ref().err().cloned())
            .or_else(|| (!errors.is_empty()).then(|| errors.join(" ; ")));
        Self {
            endpoint,
            refreshed_at,
            chain,
            network,
            mempool,
            peers,
            warnings,
            last_error,
        }
    }

    pub fn endpoint_label(&self) -> String {
        self.endpoint.label()
    }

    pub fn is_ready(&self) -> bool {
        matches!(&self.chain, Ok(chain) if !chain.initial_block_download && chain.blocks == chain.headers)
            && self.network.is_ok()
            && self.mempool.is_ok()
    }
}

const CONSOLE_CAPACITY: usize = 1000;

pub struct App {
    pub config: Config,
    pub tab: Tab,
    pub next_refresh: Instant,
    pub interval: Duration,
    pub exit: bool,
    pub show_help: bool,
    pub snapshot: Snapshot,
    pub mempool_tx_history: VecDeque<u64>,
    pub mempool_fee_history: VecDeque<f64>,
    pub peer_table_scroll: usize,
    pub node_child: Option<std::process::Child>,
    pub node_exit_code: Option<i32>,
    pub log_rx: Option<std::sync::mpsc::Receiver<String>>,
    pub console_lines: VecDeque<String>,
    pub console_scroll: usize,
    pub spawn_attempted: bool,
    pub spawn_error: Option<String>,
    pub startup_height: Option<u64>,
    pub command_input: String,
    pub cmd_rx: Option<std::sync::mpsc::Receiver<String>>,
}

impl App {
    pub fn new(config: Config) -> Self {
        let interval = config.interval;
        Self {
            config,
            tab: Tab::Dashboard,
            next_refresh: Instant::now(),
            interval,
            exit: false,
            show_help: false,
            snapshot: Snapshot::empty(),
            mempool_tx_history: VecDeque::with_capacity(HISTORY_CAPACITY),
            mempool_fee_history: VecDeque::with_capacity(HISTORY_CAPACITY),
            peer_table_scroll: 0,
            node_child: None,
            node_exit_code: None,
            log_rx: None,
            console_lines: VecDeque::with_capacity(CONSOLE_CAPACITY),
            console_scroll: 0,
            spawn_attempted: false,
            spawn_error: None,
            startup_height: None,
            command_input: String::new(),
            cmd_rx: None,
        }
    }

    pub fn refresh(&mut self) {
        self.snapshot = Snapshot::fetch(&self.config, DEFAULT_TIMEOUT);

        // Replace the dashboard summary error with a node-state-aware message
        // so the user knows whether we're starting, crashed, or connected.
        if !self.config.has_external_rpc() && self.snapshot.last_error.is_some() {
            let msg = if let Some(ref e) = self.spawn_error {
                e.clone()
            } else if let Some(code) = self.node_exit_code {
                format!("node exited with code {code} — check Console tab")
            } else if self.node_child.is_some() {
                "node is starting…".into()
            } else {
                return;
            };
            self.snapshot.last_error = Some(msg);
        }

        if let Ok(ref m) = self.snapshot.mempool {
            if self.mempool_tx_history.len() >= HISTORY_CAPACITY {
                self.mempool_tx_history.pop_front();
            }
            self.mempool_tx_history.push_back(m.transactions);
            if self.mempool_fee_history.len() >= HISTORY_CAPACITY {
                self.mempool_fee_history.pop_front();
            }
            self.mempool_fee_history.push_back(m.min_fee_sat_vb);
        }
        if let Some(ref rx) = self.log_rx {
            while let Ok(line) = rx.try_recv() {
                if self.console_lines.len() >= CONSOLE_CAPACITY {
                    self.console_lines.pop_front();
                }
                self.console_lines.push_back(line.clone());
                if let Some(h) = extract_height(&line) {
                    self.startup_height = Some(h);
                }
            }
        }
        if let Some(ref rx) = self.cmd_rx {
            if let Ok(result) = rx.try_recv() {
                for line in result.lines() {
                    if self.console_lines.len() >= CONSOLE_CAPACITY {
                        self.console_lines.pop_front();
                    }
                    self.console_lines.push_back(line.to_string());
                }
                self.cmd_rx = None;
            }
        }
        self.next_refresh = Instant::now() + self.interval;
    }

    pub fn handle_event(&mut self, event: Event) -> bool {
        match event {
            Event::Key(KeyEvent {
                code: KeyCode::Char('q'),
                ..
            })
            | Event::Key(KeyEvent {
                code: KeyCode::Esc, ..
            })
            | Event::Key(KeyEvent {
                code: KeyCode::Char('c'),
                modifiers: KeyModifiers::CONTROL,
                ..
            }) => {
                if self.show_help {
                    self.show_help = false;
                    false
                } else {
                    self.exit = true;
                    true
                }
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('r'),
                ..
            }) => {
                self.refresh();
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('h'),
                ..
            })
            | Event::Key(KeyEvent {
                code: KeyCode::Char('?'),
                ..
            }) => {
                self.show_help = !self.show_help;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Tab,
                ..
            })
            | Event::Key(KeyEvent {
                code: KeyCode::Right,
                ..
            }) => {
                self.tab = self.tab.next();
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::BackTab,
                modifiers: KeyModifiers::SHIFT,
                ..
            })
            | Event::Key(KeyEvent {
                code: KeyCode::Left,
                ..
            }) => {
                self.tab = self.tab.prev();
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('1'),
                ..
            }) => {
                self.tab = Tab::Dashboard;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('2'),
                ..
            }) => {
                self.tab = Tab::Chain;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('3'),
                ..
            }) => {
                self.tab = Tab::Network;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('4'),
                ..
            }) => {
                self.tab = Tab::Mempool;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('5'),
                ..
            }) => {
                self.tab = Tab::Console;
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Down,
                ..
            }) => {
                match self.tab {
                    Tab::Network => self.peer_table_scroll = self.peer_table_scroll.saturating_add(1),
                    Tab::Console => self.console_scroll = self.console_scroll.saturating_add(1),
                    _ => {}
                }
                false
            }
            Event::Key(KeyEvent {
                code: KeyCode::Up,
                ..
            }) => {
                match self.tab {
                    Tab::Network => self.peer_table_scroll = self.peer_table_scroll.saturating_sub(1),
                    Tab::Console => self.console_scroll = self.console_scroll.saturating_sub(1),
                    _ => {}
                }
                false
            }
            _ => false,
        }
    }
}

impl Config {
    pub fn endpoints(&self) -> Vec<RpcEndpoint> {
        if let Some(url) = &self.rpc_url {
            let (host, port) = parse_http_url(url).unwrap_or_else(|e| panic!("{e}"));
            return vec![RpcEndpoint::Tcp {
                host,
                port,
                token_file: self
                    .rpc_token_file
                    .clone()
                    .unwrap_or_else(|| self.datadir.join("rpc.token")),
            }];
        }
        if let Some(sock) = &self.rpc_socket {
            return vec![RpcEndpoint::Unix(sock.clone())];
        }
        let mut endpoints = Vec::new();
        let unix = self.datadir.join("rpc.sock");
        if unix.exists() {
            endpoints.push(RpcEndpoint::Unix(unix));
        }
        endpoints.push(RpcEndpoint::Tcp {
            host: "127.0.0.1".into(),
            port: 8332,
            token_file: self
                .rpc_token_file
                .clone()
                .unwrap_or_else(|| self.datadir.join("rpc.token")),
        });
        endpoints.push(RpcEndpoint::Unix(self.datadir.join("rpc.sock")));
        endpoints
    }
}

/// Scrape a block height from a node stderr line.
/// Looks for `height=NNNN` or `blocks=NNNN` tokens.
fn extract_height(line: &str) -> Option<u64> {
    for token in line.split_whitespace() {
        if let Some(val) = token.strip_prefix("height=") {
            if let Ok(h) = val.parse::<u64>() {
                return Some(h);
            }
        }
        if let Some(val) = token.strip_prefix("blocks=") {
            if let Ok(h) = val.parse::<u64>() {
                return Some(h);
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summary_marks_ready_only_when_chain_is_caught_up() {
        let snapshot = Snapshot {
            endpoint: RpcEndpoint::Unix(PathBuf::from("/tmp/rpc.sock")),
            refreshed_at: Some(Instant::now()),
            chain: Ok(BlockchainInfo {
                chain: "main".into(),
                blocks: 10,
                headers: 10,
                verification_progress: 1.0,
                initial_block_download: false,
                warnings: None,
            }),
            network: Ok(NetworkInfo {
                connections_in: 8,
                connections_out: 4,
                timeoffset: 0,
                warnings: None,
            }),
            mempool: Ok(MempoolInfo {
                transactions: 1,
                bytes: 2,
                maxmempool: 3,
                min_fee_sat_vb: 1.0,
                unbroadcast: 0,
            }),
            peers: Ok(vec![]),
            warnings: None,
            last_error: None,
        };
        assert!(snapshot.is_ready());
    }

    #[test]
    fn tab_navigation_cycles() {
        let mut t = Tab::Dashboard;
        t = t.next();
        assert_eq!(t, Tab::Chain);
        t = t.next();
        assert_eq!(t, Tab::Network);
        t = t.next();
        assert_eq!(t, Tab::Mempool);
        t = t.next();
        assert_eq!(t, Tab::Console);
        t = t.next();
        assert_eq!(t, Tab::Dashboard);

        t = t.prev();
        assert_eq!(t, Tab::Console);
        t = t.prev();
        assert_eq!(t, Tab::Mempool);
        t = t.prev();
        assert_eq!(t, Tab::Network);
    }
}
