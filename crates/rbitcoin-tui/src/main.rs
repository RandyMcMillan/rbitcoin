use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;
use serde_json::Value;
use std::fmt::Write as _;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const DEFAULT_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

fn main() -> ExitCode {
    match run(std::env::args_os()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<(), String> {
    let config = Config::parse(args)?;
    if config.once {
        let snapshot = Snapshot::fetch(&config, DEFAULT_TIMEOUT);
        println!("{}", snapshot.summary());
        return Ok(());
    }

    let mut terminal = Terminal::new(CrosstermBackend::new(std::io::stdout()))
        .map_err(|e| format!("terminal: {e}"))?;
    enable_raw_mode().map_err(|e| format!("raw mode: {e}"))?;
    if let Err(e) = crossterm::execute!(terminal.backend_mut(), EnterAlternateScreen) {
        let _ = disable_raw_mode();
        return Err(format!("alternate screen: {e}"));
    }
    let guard = TerminalGuard;

    let mut app = App::new(config);
    app.refresh();
    terminal
        .draw(|frame| render(frame, &app))
        .map_err(|e| format!("draw: {e}"))?;

    while !app.exit {
        let mut redraw = false;
        let wait = app
            .next_refresh
            .saturating_duration_since(Instant::now())
            .min(app.interval);
        if event::poll(wait).map_err(|e| format!("event poll: {e}"))? {
            let event = event::read().map_err(|e| format!("event read: {e}"))?;
            if app.handle_event(event) {
                break;
            }
            redraw = true;
        }
        if Instant::now() >= app.next_refresh {
            app.refresh();
            redraw = true;
        }
        if redraw {
            terminal
                .draw(|frame| render(frame, &app))
                .map_err(|e| format!("draw: {e}"))?;
        }
    }

    drop(guard);
    Ok(())
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let mut stdout = std::io::stdout();
        let _ = crossterm::execute!(stdout, LeaveAlternateScreen);
        let _ = crossterm::execute!(stdout, crossterm::cursor::Show);
    }
}

#[derive(Clone, Debug)]
struct Config {
    datadir: PathBuf,
    rpc_socket: Option<PathBuf>,
    rpc_url: Option<String>,
    rpc_token_file: Option<PathBuf>,
    interval: Duration,
    once: bool,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Self, String> {
        let mut datadir = PathBuf::from(".").join("datadir");
        let mut rpc_socket = None;
        let mut rpc_url = None;
        let mut rpc_token_file = None;
        let mut interval = DEFAULT_INTERVAL;
        let mut once = false;

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
        })
    }

    fn endpoint(&self) -> RpcEndpoint {
        if let Some(url) = &self.rpc_url {
            let (host, port) = parse_http_url(url).unwrap_or_else(|e| {
                panic!("{e}")
            });
            RpcEndpoint::Tcp {
                host,
                port,
                token_file: self
                    .rpc_token_file
                    .clone()
                    .unwrap_or_else(|| self.datadir.join("rpc.token")),
            }
        } else {
            RpcEndpoint::Unix(
                self.rpc_socket
                    .clone()
                    .unwrap_or_else(|| self.datadir.join("rpc.sock")),
            )
        }
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

fn parse_http_url(url: &str) -> Result<(String, u16), String> {
    let rest = url
        .strip_prefix("http://")
        .or_else(|| url.strip_prefix("https://"))
        .unwrap_or(url);
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| format!("--rpc-url needs host:port (got {url})"))?;
    let port = port
        .parse::<u16>()
        .map_err(|_| format!("invalid --rpc-url port in {url}"))?;
    if host.is_empty() {
        return Err(format!("invalid --rpc-url host in {url}"));
    }
    Ok((host.to_string(), port))
}

struct App {
    config: Config,
    next_refresh: Instant,
    interval: Duration,
    exit: bool,
    snapshot: Snapshot,
}

impl App {
    fn new(config: Config) -> Self {
        let interval = config.interval;
        Self {
            config,
            next_refresh: Instant::now(),
            interval,
            exit: false,
            snapshot: Snapshot::empty(),
        }
    }

    fn refresh(&mut self) {
        self.snapshot = Snapshot::fetch(&self.config, DEFAULT_TIMEOUT);
        self.next_refresh = Instant::now() + self.interval;
    }

    fn handle_event(&mut self, event: Event) -> bool {
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
                self.exit = true;
                true
            }
            Event::Key(KeyEvent {
                code: KeyCode::Char('r'),
                ..
            }) => {
                self.refresh();
                false
            }
            _ => false,
        }
    }
}

fn render(frame: &mut ratatui::Frame<'_>, app: &App) {
    let areas = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(4),
        ])
        .split(frame.area());

    frame.render_widget(summary_panel(app), areas[0]);
    frame.render_widget(chain_panel(app), areas[1]);
    frame.render_widget(net_panel(app), areas[2]);
}

fn summary_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    let _ = writeln!(text, "version: {}", env!("CARGO_PKG_VERSION"));
    let _ = writeln!(text, "endpoint: {}", app.snapshot.endpoint_label());
    let _ = writeln!(
        text,
        "last refresh: {}",
        app.snapshot
            .refreshed_at
            .map(|t| format_elapsed(t.elapsed()))
            .unwrap_or_else(|| "not yet refreshed".into())
    );
    let _ = writeln!(
        text,
        "ready: {}",
        if app.snapshot.is_ready() { "yes" } else { "no" }
    );
    if let Some(w) = &app.snapshot.warnings {
        let _ = writeln!(text, "warnings: {w}");
    }
    if let Some(err) = &app.snapshot.last_error {
        let _ = writeln!(text, "error: {err}");
    }

    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Summary"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
}

fn chain_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    match &app.snapshot.chain {
        Ok(chain) => {
            let _ = writeln!(text, "chain: {}", chain.chain);
            let _ = writeln!(text, "blocks: {}", chain.blocks);
            let _ = writeln!(text, "headers: {}", chain.headers);
            let _ = writeln!(text, "verification: {:.2}%", chain.verification_progress * 100.0);
            let _ = writeln!(
                text,
                "initial block download: {}",
                if chain.initial_block_download { "yes" } else { "no" }
            );
            let _ = writeln!(text, "tip time: {}",
                chain.time.map(format_unix_time).unwrap_or_else(|| "unknown".into())
            );
        }
        Err(err) => {
            let _ = writeln!(text, "chain error: {err}");
        }
    }

    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Chain"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
}

fn net_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    match &app.snapshot.network {
        Ok(network) => {
            let _ = writeln!(text, "connections in: {}", network.connections_in);
            let _ = writeln!(text, "connections out: {}", network.connections_out);
            let _ = writeln!(text, "time offset: {}s", network.timeoffset);
        }
        Err(err) => {
            let _ = writeln!(text, "network error: {err}");
        }
    }
    let _ = writeln!(text);
    match &app.snapshot.mempool {
        Ok(mempool) => {
            let _ = writeln!(text, "mempool txs: {}", mempool.transactions);
            let _ = writeln!(text, "mempool bytes: {}", mempool.bytes);
            let _ = writeln!(text, "max mempool: {}", mempool.maxmempool);
            let _ = writeln!(text, "min fee: {:.2} sat/vB", mempool.min_fee_sat_vb);
            let _ = writeln!(text, "unbroadcast: {}", mempool.unbroadcast);
        }
        Err(err) => {
            let _ = writeln!(text, "mempool error: {err}");
        }
    }
    let _ = writeln!(text);
    let _ = writeln!(text, "keys: q / Esc / Ctrl-C quit, r refresh");
    let _ = write!(
        text,
        "refresh: every {}s",
        app.interval.as_secs()
    );

    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Network + Mempool"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
}

fn format_elapsed(duration: Duration) -> String {
    let secs = duration.as_secs();
    let mins = secs / 60;
    let hours = mins / 60;
    let secs = secs % 60;
    let mins = mins % 60;
    if hours > 0 {
        format!("{hours}h{mins:02}m{secs:02}s")
    } else if mins > 0 {
        format!("{mins}m{secs:02}s")
    } else {
        format!("{secs}s")
    }
}

fn format_unix_time(secs: u64) -> String {
    secs.to_string()
}

#[derive(Clone, Debug)]
struct Snapshot {
    endpoint: RpcEndpoint,
    refreshed_at: Option<Instant>,
    chain: Result<BlockchainInfo, String>,
    network: Result<NetworkInfo, String>,
    mempool: Result<MempoolInfo, String>,
    warnings: Option<String>,
    last_error: Option<String>,
}

impl Snapshot {
    fn empty() -> Self {
        Self {
            endpoint: RpcEndpoint::Unix(PathBuf::from(".")),
            refreshed_at: None,
            chain: Err("waiting".into()),
            network: Err("waiting".into()),
            mempool: Err("waiting".into()),
            warnings: None,
            last_error: None,
        }
    }

    fn fetch(config: &Config, timeout: Duration) -> Self {
        let endpoint = config.endpoint();
        let refreshed_at = Some(Instant::now());
        let chain = rpc_call(&endpoint, "getblockchaininfo", &[], timeout)
            .and_then(BlockchainInfo::from_value);
        let network = rpc_call(&endpoint, "getnetworkinfo", &[], timeout)
            .and_then(NetworkInfo::from_value);
        let mempool = rpc_call(&endpoint, "getmempoolinfo", &[], timeout)
            .and_then(MempoolInfo::from_value);
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
            .or_else(|| mempool.as_ref().err().cloned());
        Self {
            endpoint,
            refreshed_at,
            chain,
            network,
            mempool,
            warnings,
            last_error,
        }
    }

    fn endpoint_label(&self) -> String {
        self.endpoint.label()
    }

    fn is_ready(&self) -> bool {
        matches!(&self.chain, Ok(chain) if !chain.initial_block_download && chain.blocks == chain.headers)
            && self.network.is_ok()
            && self.mempool.is_ok()
    }

    fn summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "endpoint: {}", self.endpoint.label());
        if let Ok(chain) = &self.chain {
            let _ = writeln!(out, "chain: {}", chain.chain);
            let _ = writeln!(out, "blocks: {}", chain.blocks);
            let _ = writeln!(out, "headers: {}", chain.headers);
            let _ = writeln!(out, "verification: {:.2}%", chain.verification_progress * 100.0);
            let _ = writeln!(
                out,
                "initial block download: {}",
                if chain.initial_block_download { "yes" } else { "no" }
            );
        } else if let Err(err) = &self.chain {
            let _ = writeln!(out, "chain: {err}");
        }
        if let Ok(network) = &self.network {
            let _ = writeln!(out, "connections: in={} out={}", network.connections_in, network.connections_out);
            let _ = writeln!(out, "timeoffset: {}s", network.timeoffset);
        } else if let Err(err) = &self.network {
            let _ = writeln!(out, "network: {err}");
        }
        if let Ok(mempool) = &self.mempool {
            let _ = writeln!(out, "mempool: txs={} bytes={} max={} min-fee={:.2} sat/vB", mempool.transactions, mempool.bytes, mempool.maxmempool, mempool.min_fee_sat_vb);
        } else if let Err(err) = &self.mempool {
            let _ = writeln!(out, "mempool: {err}");
        }
        out
    }
}

#[derive(Clone, Debug)]
enum RpcEndpoint {
    Unix(PathBuf),
    Tcp {
        host: String,
        port: u16,
        token_file: PathBuf,
    },
}

impl RpcEndpoint {
    fn label(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix {}", path.display()),
            Self::Tcp { host, port, .. } => format!("tcp {host}:{port}"),
        }
    }
}

#[derive(Clone, Debug)]
struct BlockchainInfo {
    chain: String,
    blocks: u64,
    headers: u64,
    verification_progress: f64,
    initial_block_download: bool,
    warnings: Option<String>,
}

impl BlockchainInfo {
    fn from_value(v: Value) -> Result<Self, String> {
        Ok(Self {
            chain: string_field(&v, "chain")?,
            blocks: u64_field(&v, "blocks")?,
            headers: u64_field(&v, "headers")?,
            verification_progress: f64_field(&v, "verificationprogress")?,
            initial_block_download: bool_field(&v, "initialblockdownload")?,
            warnings: optional_string_field(&v, "warnings")?,
        })
    }
}

#[derive(Clone, Debug)]
struct NetworkInfo {
    connections_in: u64,
    connections_out: u64,
    timeoffset: i64,
    warnings: Option<String>,
}

impl NetworkInfo {
    fn from_value(v: Value) -> Result<Self, String> {
        Ok(Self {
            connections_in: u64_field(&v, "connections_in")?,
            connections_out: u64_field(&v, "connections_out")?,
            timeoffset: i64_field(&v, "timeoffset")?,
            warnings: optional_string_field(&v, "warnings")?,
        })
    }
}

#[derive(Clone, Debug)]
struct MempoolInfo {
    transactions: u64,
    bytes: u64,
    maxmempool: u64,
    min_fee_sat_vb: f64,
    unbroadcast: u64,
}

impl MempoolInfo {
    fn from_value(v: Value) -> Result<Self, String> {
        let min_fee_btc_kvb = f64_field(&v, "mempoolminfee")?;
        Ok(Self {
            transactions: u64_field(&v, "size")?,
            bytes: u64_field(&v, "bytes")?,
            maxmempool: u64_field(&v, "maxmempool")?,
            min_fee_sat_vb: min_fee_btc_kvb * 100_000_000.0 / 1000.0,
            unbroadcast: u64_field(&v, "unbroadcastcount")?,
        })
    }
}

fn string_field(v: &Value, key: &str) -> Result<String, String> {
    v.get(key)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("RPC response missing string field `{key}`"))
}

fn optional_string_field(v: &Value, key: &str) -> Result<Option<String>, String> {
    match v.get(key) {
        Some(val) if val.is_null() => Ok(None),
        Some(val) => val
            .as_str()
            .map(|s| Some(s.to_owned()))
            .ok_or_else(|| format!("RPC field `{key}` is not a string or null")),
        None => Ok(None),
    }
}

fn u64_field(v: &Value, key: &str) -> Result<u64, String> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("RPC response missing integer field `{key}`"))
}

fn i64_field(v: &Value, key: &str) -> Result<i64, String> {
    v.get(key)
        .and_then(Value::as_i64)
        .ok_or_else(|| format!("RPC response missing integer field `{key}`"))
}

fn f64_field(v: &Value, key: &str) -> Result<f64, String> {
    v.get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| format!("RPC response missing numeric field `{key}`"))
}

fn bool_field(v: &Value, key: &str) -> Result<bool, String> {
    v.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| format!("RPC response missing bool field `{key}`"))
}

fn rpc_call(endpoint: &RpcEndpoint, method: &str, params: &[Value], timeout: Duration) -> Result<Value, String> {
    let body = serde_json::json!({
        "jsonrpc": "1.0",
        "id": "1",
        "method": method,
        "params": params,
    });
    let body = serde_json::to_vec(&body).map_err(|e| e.to_string())?;
    let result = match endpoint {
        RpcEndpoint::Unix(path) => {
            #[cfg(unix)]
            {
                let stream = UnixStream::connect(path)
                    .map_err(|e| format!("connect {}: {e}", path.display()))?;
                stream.set_read_timeout(Some(timeout)).ok();
                stream.set_write_timeout(Some(timeout)).ok();
                rpc_http(stream, "localhost", 0, None, &body)
            }
            #[cfg(not(unix))]
            {
                Err("unix sockets are not supported on this platform".into())
            }
        }
        RpcEndpoint::Tcp {
            host,
            port,
            token_file,
        } => {
            let stream = TcpStream::connect((host.as_str(), *port))
                .map_err(|e| format!("connect {host}:{port}: {e}"))?;
            stream.set_read_timeout(Some(timeout)).ok();
            stream.set_write_timeout(Some(timeout)).ok();
            let token = read_token(token_file)?;
            rpc_http(stream, host, *port, Some(&token), &body)
        }
    }?;
    let v: Value = serde_json::from_str(&result).map_err(|e| format!("rpc json: {e}"))?;
    if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
        let msg = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("rpc error");
        let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
        return Err(format!("RPC error {code}: {msg}"));
    }
    Ok(v.get("result").cloned().unwrap_or(Value::Null))
}

fn rpc_http(
    mut stream: impl Read + Write,
    host: &str,
    port: u16,
    auth: Option<&str>,
    body: &[u8],
) -> Result<String, String> {
    let auth_h = match auth {
        Some(token) => {
            let req = format!(
                "POST / HTTP/1.1\r\nHost: {host}:{port}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let mut wire = req.into_bytes();
            wire.extend_from_slice(body);
            stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
            let mut buf = Vec::new();
            stream
                .read_to_end(&mut buf)
                .map_err(|e| format!("read: {e}"))?;
            let text = String::from_utf8(buf).map_err(|e| format!("utf-8: {e}"))?;
            return parse_http_response(&text);
        }
        None => "",
    };
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {host}:{port}\r\n{auth_h}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut wire = req.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8(buf).map_err(|e| format!("utf-8: {e}"))?;
    parse_http_response(&text)
}

#[allow(dead_code)]
fn rpc_http_with_auth(
    mut stream: impl Read + Write,
    host: &str,
    port: u16,
    _token: &str,
    body: &[u8],
) -> Result<String, String> {
    let req = format!(
        "POST / HTTP/1.1\r\nHost: {host}:{port}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut wire = req.into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).map_err(|e| format!("write: {e}"))?;
    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("read: {e}"))?;
    let text = String::from_utf8(buf).map_err(|e| format!("utf-8: {e}"))?;
    parse_http_response(&text)
}

fn parse_http_response(text: &str) -> Result<String, String> {
    let (head, body) = text
        .split_once("\r\n\r\n")
        .or_else(|| text.split_once("\n\n"))
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if status == 401 {
        return Err("RPC unauthorized (check rpc.token or --rpc-token-file)".into());
    }
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status}: {body}"));
    }
    Ok(body.to_string())
}

fn read_token(path: &PathBuf) -> Result<String, String> {
    let line =
        std::fs::read_to_string(path).map_err(|e| format!("read token {}: {e}", path.display()))?;
    let token = line.trim();
    if token.is_empty() {
        return Err(format!("token file {}: empty", path.display()));
    }
    Ok(token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_http_url_accepts_plain_or_schemed_urls() {
        assert_eq!(
            parse_http_url("http://127.0.0.1:8332").expect("url"),
            ("127.0.0.1".into(), 8332)
        );
        assert_eq!(
            parse_http_url("127.0.0.1:18332").expect("url"),
            ("127.0.0.1".into(), 18332)
        );
    }

    #[test]
    fn parse_http_response_accepts_success_body() {
        let body = parse_http_response(
            "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello",
        )
        .expect("body");
        assert_eq!(body, "hello");
    }

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
            warnings: None,
            last_error: None,
        };
        assert!(snapshot.is_ready());
        assert!(snapshot.summary().contains("chain: main"));
    }
}
