use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::style::{Color, Style};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;
use rbitcoin_primitives::DEFAULT_HEALTH_PORT;
use std::fmt::Write as _;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::process::ExitCode;
use std::str::FromStr;
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
        let snapshot = Snapshot::fetch(config.addr, DEFAULT_TIMEOUT);
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

    let mut app = App::new(config.addr, config.interval);
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
    addr: SocketAddr,
    interval: Duration,
    once: bool,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Self, String> {
        let mut addr = SocketAddr::from(([127, 0, 0, 1], DEFAULT_HEALTH_PORT));
        let mut interval = DEFAULT_INTERVAL;
        let mut once = false;
        let mut positional = None;

        let mut iter = args.into_iter();
        let _ = iter.next();
        while let Some(arg) = iter.next() {
            let arg = arg.to_string_lossy().into_owned();
            match arg.as_str() {
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                "-a" | "--addr" => {
                    let value = iter.next().ok_or("--addr requires a SocketAddr")?;
                    addr = parse_addr(&value)?;
                }
                "-i" | "--interval" => {
                    let value = iter.next().ok_or("--interval requires seconds")?;
                    interval = Duration::from_secs(parse_u64(&value, "--interval")?);
                    if interval.is_zero() {
                        return Err("--interval must be at least 1 second".into());
                    }
                }
                "--once" => once = true,
                other if other.starts_with('-') => return Err(format!("unknown flag `{other}`")),
                other if positional.is_none() => positional = Some(other.to_string()),
                other => return Err(format!("unexpected argument `{other}`")),
            }
        }

        if let Some(value) = positional {
            addr = SocketAddr::from_str(&value)
                .map_err(|e| format!("invalid health address `{value}`: {e}"))?;
        }

        Ok(Self {
            addr,
            interval,
            once,
        })
    }
}

const USAGE: &str = "\
rbitcoin-tui [--addr ADDR] [--interval SECONDS] [--once] [ADDR]

TUI client for an existing rbitcoin node health / ready / progress surface.

Options:
  -a, --addr ADDR       Existing node health listener address (default 127.0.0.1:9332)
  -i, --interval SEC    Refresh interval in seconds (default 1)
      --once            Fetch a single snapshot and print it
  -h, --help            Show this help

ADDR may also be passed positionally as a convenience.
";

fn parse_addr(value: &std::ffi::OsString) -> Result<SocketAddr, String> {
    let value = value.to_string_lossy();
    SocketAddr::from_str(&value).map_err(|e| format!("invalid addr `{value}`: {e}"))
}

fn parse_u64(value: &std::ffi::OsString, flag: &str) -> Result<u64, String> {
    let value = value.to_string_lossy();
    value
        .parse::<u64>()
        .map_err(|e| format!("invalid {flag} `{value}`: {e}"))
}

struct App {
    addr: SocketAddr,
    interval: Duration,
    next_refresh: Instant,
    exit: bool,
    healthz: Probe,
    readyz: Probe,
    progress: Option<ProgressView>,
    progress_error: Option<String>,
    last_refresh: Option<Instant>,
}

impl App {
    fn new(addr: SocketAddr, interval: Duration) -> Self {
        Self {
            addr,
            interval,
            next_refresh: Instant::now(),
            exit: false,
            healthz: Probe::waiting(),
            readyz: Probe::waiting(),
            progress: None,
            progress_error: None,
            last_refresh: None,
        }
    }

    fn refresh(&mut self) {
        let snapshot = Snapshot::fetch(self.addr, DEFAULT_TIMEOUT);
        self.healthz = snapshot.healthz;
        self.readyz = snapshot.readyz;
        self.progress = snapshot.progress;
        self.progress_error = snapshot.progress_error;
        self.last_refresh = Some(Instant::now());
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
                let _ = self.refresh();
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
            Constraint::Length(9),
            Constraint::Length(10),
            Constraint::Min(3),
        ])
        .split(frame.area());

    frame.render_widget(node_panel(app), areas[0]);
    frame.render_widget(progress_panel(app), areas[1]);
    frame.render_widget(help_panel(app), areas[2]);
}

fn node_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    let _ = writeln!(text, "version: {}", env!("CARGO_PKG_VERSION"));
    let _ = writeln!(text, "endpoint: {}", app.addr);
    let _ = writeln!(
        text,
        "last refresh: {}",
        app.last_refresh
            .map(|t| format_duration(t.elapsed()))
            .unwrap_or_else(|| "not yet refreshed".into())
    );
    let _ = writeln!(text);
    let _ = writeln!(text, "healthz: {}", app.healthz.status_line());
    let _ = writeln!(text, "  {}", app.healthz.body_line());
    let _ = writeln!(text, "readyz: {}", app.readyz.status_line());
    let _ = write!(text, "  {}", app.readyz.body_line());

    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Node"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
}

fn progress_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    if let Some(progress) = &app.progress {
        let _ = writeln!(text, "phase: {}", progress.phase);
        let _ = writeln!(
            text,
            "stage: {}",
            progress.stage.as_deref().unwrap_or("none")
        );
        let _ = writeln!(
            text,
            "done: {}",
            progress
                .done
                .map(|done| match progress.total {
                    Some(total) => format!("{done} / {total}"),
                    None => done.to_string(),
                })
                .unwrap_or_else(|| "unknown".into())
        );
        let _ = writeln!(
            text,
            "percent: {}",
            progress
                .percent
                .map(|p| format!("{p:.2}%"))
                .unwrap_or_else(|| "unknown".into())
        );
        let _ = writeln!(
            text,
            "elapsed: {}",
            progress
                .elapsed_secs
                .map(|secs| format_duration(Duration::from_secs(secs)))
                .unwrap_or_else(|| "unknown".into())
        );
        let _ = writeln!(
            text,
            "eta: {}",
            progress
                .eta_secs
                .map(|secs| format_duration(Duration::from_secs(secs)))
                .unwrap_or_else(|| "unknown".into())
        );
        if let Some(finished) = &progress.finished {
            let _ = write!(
                text,
                "finished: {} done={} total={} elapsed={}",
                finished.stage,
                finished.done,
                finished.total,
                format_duration(Duration::from_secs(finished.elapsed_secs))
            );
        }
    } else if let Some(err) = &app.progress_error {
        let _ = writeln!(text, "progress: {err}");
    } else {
        let _ = writeln!(text, "progress: waiting for the first snapshot");
    }

    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Progress"))
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
}

fn help_panel(app: &App) -> Paragraph<'static> {
    let mut text = String::new();
    let _ = writeln!(text, "keys: q / Esc / Ctrl-C quit, r refresh");
    let _ = writeln!(text, "auto-refresh: {}s", app.interval.as_secs());
    if let Some(err) = &app.healthz.error {
        let _ = writeln!(text, "health error: {err}");
    }
    if let Some(err) = &app.readyz.error {
        let _ = writeln!(text, "ready error: {err}");
    }
    Paragraph::new(text)
        .block(Block::default().borders(Borders::ALL).title("Help"))
        .style(Style::default().fg(Color::DarkGray))
        .wrap(Wrap { trim: true })
}

fn format_duration(duration: Duration) -> String {
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

#[derive(Clone, Debug, Default)]
struct Probe {
    status: String,
    body: String,
    error: Option<String>,
}

impl Probe {
    fn waiting() -> Self {
        Self {
            status: "waiting".into(),
            body: "no response yet".into(),
            error: None,
        }
    }

    fn error(err: String) -> Self {
        Self {
            status: "error".into(),
            body: err.clone(),
            error: Some(err),
        }
    }

    fn from_http(addr: SocketAddr, path: &str, timeout: Duration) -> Result<Self, String> {
        let response = http_get(addr, path, timeout)?;
        Ok(Self {
            status: format!("{} {}", response.code, response.reason),
            body: response.body.trim().to_string(),
            error: None,
        })
    }

    fn status_line(&self) -> String {
        if let Some(err) = &self.error {
            format!("error: {err}")
        } else {
            self.status.clone()
        }
    }

    fn body_line(&self) -> String {
        if let Some(err) = &self.error {
            err.clone()
        } else {
            self.body.clone()
        }
    }
}

#[derive(Clone, Debug)]
struct Snapshot {
    healthz: Probe,
    readyz: Probe,
    progress: Option<ProgressView>,
    progress_error: Option<String>,
}

impl Snapshot {
    fn fetch(addr: SocketAddr, timeout: Duration) -> Self {
        let healthz = Probe::from_http(addr, "/healthz", timeout).unwrap_or_else(Probe::error);
        let readyz = Probe::from_http(addr, "/readyz", timeout).unwrap_or_else(Probe::error);
        let (progress, progress_error) = match http_get(addr, "/progress", timeout) {
            Ok(response) if response.code == 200 => match ProgressView::from_json(&response.body) {
                Ok(progress) => (Some(progress), None),
                Err(err) => (None, Some(err)),
            },
            Ok(response) => (
                None,
                Some(format!(
                    "HTTP {} {}: {}",
                    response.code,
                    response.reason,
                    response.body.trim()
                )),
            ),
            Err(err) => (None, Some(err)),
        };
        Self {
            healthz,
            readyz,
            progress,
            progress_error,
        }
    }

    fn summary(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "healthz: {}", self.healthz.status_line());
        let _ = writeln!(out, "  {}", self.healthz.body_line());
        let _ = writeln!(out, "readyz: {}", self.readyz.status_line());
        let _ = writeln!(out, "  {}", self.readyz.body_line());
        if let Some(progress) = &self.progress {
            let _ = writeln!(out, "phase: {}", progress.phase);
            let _ = writeln!(
                out,
                "stage: {}",
                progress.stage.as_deref().unwrap_or("none")
            );
            let _ = writeln!(
                out,
                "done: {}",
                progress
                    .done
                    .map(|d| d.to_string())
                    .unwrap_or_else(|| "unknown".into())
            );
            let _ = writeln!(
                out,
                "percent: {}",
                progress
                    .percent
                    .map(|p| format!("{p:.2}%"))
                    .unwrap_or_else(|| "unknown".into())
            );
        } else if let Some(err) = &self.progress_error {
            let _ = writeln!(out, "progress: {err}");
        }
        out
    }
}

#[derive(Clone, Debug)]
struct ProgressView {
    phase: String,
    stage: Option<String>,
    done: Option<u64>,
    total: Option<u64>,
    percent: Option<f64>,
    elapsed_secs: Option<u64>,
    eta_secs: Option<u64>,
    finished: Option<FinishedView>,
}

#[derive(Clone, Debug)]
struct FinishedView {
    stage: String,
    done: u64,
    total: u64,
    elapsed_secs: u64,
}

impl ProgressView {
    fn from_json(body: &str) -> Result<Self, String> {
        let value: serde_json::Value =
            serde_json::from_str(body).map_err(|e| format!("progress JSON: {e}"))?;
        let phase = string_field(&value, "phase")?;
        let stage = optional_string_field(&value, "stage")?;
        let done = optional_u64_field(&value, "done")?;
        let total = optional_u64_field(&value, "total")?;
        let percent = optional_f64_field(&value, "percent")?;
        let elapsed_secs = optional_u64_field(&value, "elapsed_secs")?;
        let eta_secs = optional_u64_field(&value, "eta_secs")?;
        let finished = match value.get("finished") {
            Some(v) if !v.is_null() => Some(FinishedView {
                stage: string_field(v, "stage")?,
                done: required_u64_field(v, "done")?,
                total: required_u64_field(v, "total")?,
                elapsed_secs: required_u64_field(v, "elapsed_secs")?,
            }),
            _ => None,
        };
        Ok(Self {
            phase,
            stage,
            done,
            total,
            percent,
            elapsed_secs,
            eta_secs,
            finished,
        })
    }
}

fn string_field(value: &serde_json::Value, key: &str) -> Result<String, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("progress JSON missing string `{key}`"))
}

fn optional_string_field(value: &serde_json::Value, key: &str) -> Result<Option<String>, String> {
    match value.get(key) {
        Some(v) if v.is_null() => Ok(None),
        Some(v) => v
            .as_str()
            .map(|s| Some(s.to_owned()))
            .ok_or_else(|| format!("progress JSON field `{key}` is not a string or null")),
        None => Ok(None),
    }
}

fn required_u64_field(value: &serde_json::Value, key: &str) -> Result<u64, String> {
    value
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| format!("progress JSON missing integer `{key}`"))
}

fn optional_u64_field(value: &serde_json::Value, key: &str) -> Result<Option<u64>, String> {
    match value.get(key) {
        Some(v) if v.is_null() => Ok(None),
        Some(v) => v
            .as_u64()
            .map(Some)
            .ok_or_else(|| format!("progress JSON field `{key}` is not an integer or null")),
        None => Ok(None),
    }
}

fn optional_f64_field(value: &serde_json::Value, key: &str) -> Result<Option<f64>, String> {
    match value.get(key) {
        Some(v) if v.is_null() => Ok(None),
        Some(v) => v
            .as_f64()
            .map(Some)
            .ok_or_else(|| format!("progress JSON field `{key}` is not a number or null")),
        None => Ok(None),
    }
}

struct HttpResponse {
    code: u16,
    reason: String,
    body: String,
}

fn http_get(addr: SocketAddr, path: &str, timeout: Duration) -> Result<HttpResponse, String> {
    let mut stream =
        TcpStream::connect_timeout(&addr, timeout).map_err(|e| format!("{addr}: {e}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| format!("read timeout: {e}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| format!("write timeout: {e}"))?;
    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nUser-Agent: rbitcoin-tui\r\nAccept: application/json, text/plain\r\n\r\n"
    );
    stream
        .write_all(req.as_bytes())
        .and_then(|_| stream.flush())
        .map_err(|e| format!("{addr} write {path}: {e}"))?;

    let mut buf = Vec::new();
    stream
        .read_to_end(&mut buf)
        .map_err(|e| format!("{addr} read {path}: {e}"))?;
    let text = String::from_utf8(buf).map_err(|e| format!("{addr} invalid utf-8 {path}: {e}"))?;
    parse_http_response(&text)
}

fn parse_http_response(text: &str) -> Result<HttpResponse, String> {
    let (head, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| "HTTP response missing header/body separator".to_string())?;
    let mut head_lines = head.lines();
    let status_line = head_lines
        .next()
        .ok_or_else(|| "HTTP response missing status line".to_string())?;
    let mut parts = status_line.splitn(3, ' ');
    let _http = parts
        .next()
        .ok_or_else(|| "HTTP response missing protocol".to_string())?;
    let code = parts
        .next()
        .ok_or_else(|| format!("bad HTTP status line: {status_line}"))?
        .parse::<u16>()
        .map_err(|e| format!("bad HTTP status code in `{status_line}`: {e}"))?;
    let reason = parts.next().unwrap_or("").trim().to_string();
    Ok(HttpResponse {
        code,
        reason,
        body: body.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_progress_json_with_finished_stage() {
        let body = r#"{
            "phase":"following",
            "stage":"tip follow",
            "done":3,
            "total":5,
            "percent":60.0,
            "elapsed_secs":9,
            "eta_secs":6,
            "finished":{"stage":"indexing","done":5,"total":5,"elapsed_secs":12}
        }"#;
        let progress = ProgressView::from_json(body).expect("progress");
        assert_eq!(progress.phase, "following");
        assert_eq!(progress.stage.as_deref(), Some("tip follow"));
        assert_eq!(progress.done, Some(3));
        assert_eq!(progress.total, Some(5));
        assert_eq!(progress.percent, Some(60.0));
        assert_eq!(progress.elapsed_secs, Some(9));
        assert_eq!(progress.eta_secs, Some(6));
        let finished = progress.finished.expect("finished");
        assert_eq!(finished.stage, "indexing");
        assert_eq!(finished.done, 5);
        assert_eq!(finished.total, 5);
        assert_eq!(finished.elapsed_secs, 12);
    }

    #[test]
    fn parse_http_response_extracts_status_and_body() {
        let response = parse_http_response(
            "HTTP/1.1 503 Service Unavailable\r\ncontent-length: 18\r\n\r\nnot ready: opening\n",
        )
        .expect("response");
        assert_eq!(response.code, 503);
        assert_eq!(response.reason, "Service Unavailable");
        assert_eq!(response.body, "not ready: opening\n");
    }

    #[test]
    fn format_duration_compacts_minutes_and_hours() {
        assert_eq!(format_duration(Duration::from_secs(9)), "9s");
        assert_eq!(format_duration(Duration::from_secs(75)), "1m15s");
        assert_eq!(
            format_duration(Duration::from_secs(3 * 3600 + 4 * 60 + 5)),
            "3h04m05s"
        );
    }
}
