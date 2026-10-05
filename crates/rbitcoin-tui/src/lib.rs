pub mod app;
pub mod config;
pub mod rpc;
pub mod ui;

use crossterm::event::{self};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::Terminal;
use std::fmt::Write as _;
use std::io;
use std::process::ExitCode;
use std::time::Instant;

pub fn tui_main(args: impl IntoIterator<Item = std::ffi::OsString>) -> ExitCode {
    match run(args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<(), String> {
    let config = config::Config::parse(args)?;
    if config.once {
        let snapshot = app::Snapshot::fetch(&config, std::time::Duration::from_secs(2));
        println!("{}", snapshot_summary(&snapshot));
        return Ok(());
    }

    let mut terminal = Terminal::new(CrosstermBackend::new(io::stdout()))
        .map_err(|e| format!("terminal: {e}"))?;
    enable_raw_mode().map_err(|e| format!("raw mode: {e}"))?;
    if let Err(e) = crossterm::execute!(terminal.backend_mut(), EnterAlternateScreen) {
        let _ = disable_raw_mode();
        return Err(format!("alternate screen: {e}"));
    }
    let guard = TerminalGuard;

    let mut app = app::App::new(config);
    app.refresh();
    terminal
        .draw(|frame| ui::render(frame, &app))
        .map_err(|e| format!("draw: {e}"))?;

    while !app.exit {
        let mut redraw = false;
        let wait = app
            .next_refresh
            .saturating_duration_since(Instant::now())
            .min(app.interval);
        if event::poll(wait).map_err(|e| format!("event poll: {e}"))? {
            let ev = event::read().map_err(|e| format!("event read: {e}"))?;
            if app.handle_event(ev) {
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
                .draw(|frame| ui::render(frame, &app))
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
        let mut stdout = io::stdout();
        let _ = crossterm::execute!(stdout, LeaveAlternateScreen);
        let _ = crossterm::execute!(stdout, crossterm::cursor::Show);
    }
}

fn snapshot_summary(snapshot: &app::Snapshot) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "endpoint: {}", snapshot.endpoint_label());
    if let Ok(chain) = &snapshot.chain {
        let _ = writeln!(out, "chain: {}", chain.chain);
        let _ = writeln!(out, "blocks: {}", chain.blocks);
        let _ = writeln!(out, "headers: {}", chain.headers);
        let _ = writeln!(out, "verification: {:.2}%", chain.verification_progress * 100.0);
        let _ = writeln!(
            out,
            "initial block download: {}",
            if chain.initial_block_download { "yes" } else { "no" }
        );
    } else if let Err(err) = &snapshot.chain {
        let _ = writeln!(out, "chain: {err}");
    }
    if let Ok(network) = &snapshot.network {
        let _ = writeln!(
            out,
            "connections: in={} out={}",
            network.connections_in,
            network.connections_out
        );
        let _ = writeln!(out, "timeoffset: {}s", network.timeoffset);
    } else if let Err(err) = &snapshot.network {
        let _ = writeln!(out, "network: {err}");
    }
    if let Ok(mempool) = &snapshot.mempool {
        let _ = writeln!(
            out,
            "mempool: txs={} bytes={} max={} min-fee={:.2} sat/vB",
            mempool.transactions,
            mempool.bytes,
            mempool.maxmempool,
            mempool.min_fee_sat_vb
        );
    } else if let Err(err) = &snapshot.mempool {
        let _ = writeln!(out, "mempool: {err}");
    }
    out
}
