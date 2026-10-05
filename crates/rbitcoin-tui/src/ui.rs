use crate::app::{App, Tab};
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Gauge, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Sparkline, Table, Wrap,
};
use ratatui::Frame;
use std::fmt::Write as _;
use std::time::Duration;

pub fn render(frame: &mut Frame<'_>, app: &App) {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(0), Constraint::Length(1)])
        .split(area);

    render_tab_bar(frame, chunks[0], app);
    render_content(frame, chunks[1], app);
    render_status_bar(frame, chunks[2], app);

    if app.show_help {
        render_help(frame, area);
    }
}

fn render_tab_bar(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let titles: Vec<Line<'_>> = Tab::ALL
        .iter()
        .map(|t| {
            let (name, key) = match t {
                Tab::Dashboard => ("Dashboard", "1"),
                Tab::Chain => ("Chain", "2"),
                Tab::Network => ("Network", "3"),
                Tab::Mempool => ("Mempool", "4"),
            };
            let style = if *t == app.tab {
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::Gray)
            };
            Line::from(vec![
                Span::styled(format!(" {key} "), style),
                Span::styled(name.to_string(), style),
                Span::styled(" ", style),
            ])
        })
        .collect();

    let spans: Vec<Span<'_>> = titles
        .into_iter()
        .enumerate()
        .flat_map(|(i, line)| {
            let mut spans = line.spans;
            if i > 0 {
                spans.insert(0, Span::raw(" "));
            }
            spans
        })
        .collect();

    let block = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(Color::DarkGray));
    let paragraph = Paragraph::new(Line::from(spans)).block(block);
    frame.render_widget(paragraph, area);
}

fn render_content(frame: &mut Frame<'_>, area: Rect, app: &App) {
    match app.tab {
        Tab::Dashboard => render_dashboard(frame, area, app),
        Tab::Chain => render_chain(frame, area, app),
        Tab::Network => render_network(frame, area, app),
        Tab::Mempool => render_mempool(frame, area, app),
    }
}

fn render_status_bar(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let endpoint = app.snapshot.endpoint_label();
    let ready = if app.snapshot.is_ready() {
        Span::styled("READY", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD))
    } else {
        Span::styled("SYNCING", Style::default().fg(Color::Yellow))
    };
    let refresh = format!("{}s", app.interval.as_secs());
    let left = Line::from(vec![
        Span::raw("endpoint: "),
        Span::styled(endpoint, Style::default().fg(Color::Cyan)),
        Span::raw(" | status: "),
        ready,
    ]);
    let right = Line::from(vec![
        Span::raw("refresh: "),
        Span::styled(refresh, Style::default().fg(Color::Cyan)),
        Span::raw(" | h help | q quit"),
    ]);

    let status = Paragraph::new(left).alignment(Alignment::Left);
    frame.render_widget(status, area);
    let status_right = Paragraph::new(right).alignment(Alignment::Right);
    frame.render_widget(status_right, area);
}

fn render_dashboard(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Length(9), Constraint::Min(0)])
        .margin(1)
        .split(area);

    let inner = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" rbitcoin-tui ")
        .title_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
    frame.render_widget(inner, area);

    render_summary_block(frame, chunks[0], app);
    render_chain_gauge_row(frame, chunks[1], app);
    render_mempool_sparkline(frame, chunks[2], app);
}

fn render_summary_block(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Summary ")
        .title_style(Style::default().fg(Color::White));

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

    let ready_text = if app.snapshot.is_ready() {
        "yes"
    } else {
        "no"
    };
    let _ = writeln!(text, "ready: {ready_text}");

    if let Some(w) = &app.snapshot.warnings {
        let _ = writeln!(text, "warnings: {w}");
    }
    if let Some(err) = &app.snapshot.last_error {
        let _ = writeln!(text, "error: {err}");
    }

    let paragraph = Paragraph::new(text)
        .block(block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true });
    frame.render_widget(paragraph, area);
}

fn render_chain_gauge_row(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    let gauge_block = Block::default()
        .borders(Borders::ALL)
        .title(" Verification Progress ")
        .title_style(Style::default().fg(Color::White));

    match &app.snapshot.chain {
        Ok(chain) => {
            let ratio = chain.verification_progress.clamp(0.0, 1.0);
            let color = if ratio >= 0.99 {
                Color::Green
            } else if ratio >= 0.5 {
                Color::Yellow
            } else {
                Color::Red
            };
            let label = format!("{:.2}%", ratio * 100.0);
            let gauge = Gauge::default()
                .block(gauge_block)
                .gauge_style(Style::default().fg(color).bg(Color::Black))
                .ratio(ratio)
                .label(label);
            frame.render_widget(gauge, chunks[0]);

            let net_block = Block::default()
                .borders(Borders::ALL)
                .title(" Network ")
                .title_style(Style::default().fg(Color::White));
            let mut net_text = String::new();
            let _ = writeln!(
                net_text,
                "in: {}  out: {}",
                app.snapshot
                    .network
                    .as_ref()
                    .map(|n| n.connections_in)
                    .unwrap_or(0),
                app.snapshot
                    .network
                    .as_ref()
                    .map(|n| n.connections_out)
                    .unwrap_or(0)
            );
            let _ = writeln!(
                net_text,
                "time offset: {}s",
                app.snapshot
                    .network
                    .as_ref()
                    .map(|n| n.timeoffset)
                    .unwrap_or(0)
            );
            let _ = writeln!(net_text, "chain: {}", chain.chain);
            let _ = writeln!(net_text, "blocks: {}", chain.blocks);
            let _ = write!(net_text, "headers: {}", chain.headers);
            let net_para = Paragraph::new(net_text)
                .block(net_block)
                .style(Style::default().fg(Color::White))
                .wrap(Wrap { trim: true });
            frame.render_widget(net_para, chunks[1]);
        }
        Err(err) => {
            let gauge = Gauge::default()
                .block(gauge_block)
                .gauge_style(Style::default().fg(Color::Red))
                .ratio(0.0)
                .label(err.clone());
            frame.render_widget(gauge, chunks[0]);

            let empty = Block::default().borders(Borders::ALL);
            frame.render_widget(empty, chunks[1]);
        }
    }
}

fn render_mempool_sparkline(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Mempool ")
        .title_style(Style::default().fg(Color::White));

    let inner = block.inner(area);
    frame.render_widget(block, area);

    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(inner);

    let data: Vec<u64> = app.mempool_tx_history.iter().copied().collect();
    if data.len() >= 2 {
        let sparkline = Sparkline::default()
            .data(&data)
            .style(Style::default().fg(Color::Cyan))
            .max(
                *data
                    .iter()
                    .max()
                    .unwrap_or(&1)
                    .max(&1),
            );
        frame.render_widget(sparkline, chunks[0]);
    } else {
        let placeholder = Paragraph::new("collecting data…")
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        frame.render_widget(placeholder, chunks[0]);
    }

    let mut text = String::new();
    match &app.snapshot.mempool {
        Ok(mempool) => {
            let _ = writeln!(text, "txs: {}", mempool.transactions);
            let _ = writeln!(text, "bytes: {}", human_bytes(mempool.bytes));
            let _ = writeln!(text, "max: {}", human_bytes(mempool.maxmempool));
            let _ = writeln!(text, "min fee: {:.2} sat/vB", mempool.min_fee_sat_vb);
            let _ = write!(text, "unbroadcast: {}", mempool.unbroadcast);
        }
        Err(err) => {
            let _ = write!(text, "{err}");
        }
    }
    let para = Paragraph::new(text)
        .style(Style::default().fg(Color::White))
        .alignment(Alignment::Right)
        .wrap(Wrap { trim: true });
    frame.render_widget(para, chunks[1]);
}

fn render_chain(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Chain ")
        .title_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    match &app.snapshot.chain {
        Ok(chain) => {
            let rows = [
                ("Chain", chain.chain.clone()),
                ("Blocks", chain.blocks.to_string()),
                ("Headers", chain.headers.to_string()),
                (
                    "Verification",
                    format!("{:.4}%", chain.verification_progress * 100.0),
                ),
                (
                    "IBD",
                    if chain.initial_block_download {
                        "yes".into()
                    } else {
                        "no".into()
                    },
                ),
            ];
            let mut text = String::new();
            for (k, v) in &rows {
                let _ = writeln!(text, "{k:<20} {v}");
            }
            let para = Paragraph::new(text)
                .style(Style::default().fg(Color::White))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, inner);
        }
        Err(err) => {
            let para = Paragraph::new(format!("Error: {err}"))
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, inner);
        }
    }
}

fn render_network(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(6), Constraint::Min(0)])
        .margin(1)
        .split(area);

    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Network ")
        .title_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
    frame.render_widget(outer, area);

    render_net_summary(frame, chunks[0], app);
    render_peer_table(frame, chunks[1], app);
}

fn render_net_summary(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Connections ")
        .title_style(Style::default().fg(Color::White));
    match &app.snapshot.network {
        Ok(net) => {
            let mut text = String::new();
            let _ = writeln!(text, "Inbound:     {}", net.connections_in);
            let _ = writeln!(text, "Outbound:    {}", net.connections_out);
            let _ = writeln!(text, "Time offset: {}s", net.timeoffset);
            if let Some(w) = &net.warnings {
                let _ = writeln!(text, "Warnings:    {w}");
            }
            let para = Paragraph::new(text)
                .block(block)
                .style(Style::default().fg(Color::White))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, area);
        }
        Err(err) => {
            let para = Paragraph::new(format!("Error: {err}"))
                .block(block)
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, area);
        }
    }
}

fn render_peer_table(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let header = Row::new(vec!["ID", "Address", "Type", "Height", "Ping", "Version"])
        .style(Style::default().fg(Color::Yellow).add_modifier(Modifier::BOLD))
        .height(1);

    match &app.snapshot.peers {
        Ok(peers) => {
            let rows: Vec<Row<'_>> = peers
                .iter()
                .map(|p| {
                    let ping = p
                        .ping_ms
                        .map(|ms| format!("{ms}ms"))
                        .unwrap_or_else(|| "-".into());
                    let conn_color = if p.inbound {
                        Color::Cyan
                    } else {
                        Color::Green
                    };
                    Row::new(vec![
                        Cell::from(p.id.to_string()),
                        Cell::from(p.addr.clone()),
                        Cell::from(p.conn_type.clone()).style(Style::default().fg(conn_color)),
                        Cell::from(p.height.to_string()),
                        Cell::from(ping),
                        Cell::from(p.subver.clone()),
                    ])
                    .height(1)
                })
                .collect();

            let table = Table::new(
                rows,
                [
                    Constraint::Length(6),
                    Constraint::Min(20),
                    Constraint::Length(12),
                    Constraint::Length(10),
                    Constraint::Length(8),
                    Constraint::Min(15),
                ],
            )
            .header(header)
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Peers ")
                    .title_style(Style::default().fg(Color::White)),
            )
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

            let visible = area.height.saturating_sub(3) as usize;
            let max_scroll = peers.len().saturating_sub(visible);
            let scroll = app.peer_table_scroll.min(max_scroll);

            let mut state = ratatui::widgets::TableState::default();
            if !peers.is_empty() {
                state.select(Some(scroll));
            }
            frame.render_stateful_widget(table, area, &mut state);

            if peers.len() > visible {
                let scrollbar = Scrollbar::default()
                    .orientation(ScrollbarOrientation::VerticalRight);
                let mut sb_state = ScrollbarState::new(peers.len()).position(scroll);
                frame.render_stateful_widget(
                    scrollbar,
                    area.inner(Margin {
                        horizontal: 0,
                        vertical: 1,
                    }),
                    &mut sb_state,
                );
            }
        }
        Err(err) => {
            let para = Paragraph::new(format!("Error: {err}"))
                .block(
                    Block::default()
                        .borders(Borders::ALL)
                        .title(" Peers "),
                )
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, area);
        }
    }
}

fn render_mempool(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(0)])
        .margin(1)
        .split(area);

    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(" Mempool ")
        .title_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));
    frame.render_widget(outer, area);

    render_mempool_gauges(frame, chunks[0], app);
    render_mempool_details(frame, chunks[1], app);
}

fn render_mempool_gauges(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    let fee_data: Vec<u64> = app
        .mempool_fee_history
        .iter()
        .map(|f| (*f).clamp(0.0, u64::MAX as f64) as u64)
        .collect();

    let fee_block = Block::default()
        .borders(Borders::ALL)
        .title(" Min Fee History (sat/vB) ")
        .title_style(Style::default().fg(Color::White));
    if fee_data.len() >= 2 {
        let sparkline = Sparkline::default()
            .data(&fee_data)
            .style(Style::default().fg(Color::Magenta))
            .max(*fee_data.iter().max().unwrap_or(&1).max(&1));
        frame.render_widget(sparkline.block(fee_block), chunks[0]);
    } else {
        let placeholder = Paragraph::new("collecting data…")
            .block(fee_block)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        frame.render_widget(placeholder, chunks[0]);
    }

    let tx_data: Vec<u64> = app.mempool_tx_history.iter().copied().collect();
    let tx_block = Block::default()
        .borders(Borders::ALL)
        .title(" Tx Count History ")
        .title_style(Style::default().fg(Color::White));
    if tx_data.len() >= 2 {
        let sparkline = Sparkline::default()
            .data(&tx_data)
            .style(Style::default().fg(Color::Cyan))
            .max(*tx_data.iter().max().unwrap_or(&1).max(&1));
        frame.render_widget(sparkline.block(tx_block), chunks[1]);
    } else {
        let placeholder = Paragraph::new("collecting data…")
            .block(tx_block)
            .style(Style::default().fg(Color::DarkGray))
            .alignment(Alignment::Center);
        frame.render_widget(placeholder, chunks[1]);
    }
}

fn render_mempool_details(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(" Details ")
        .title_style(Style::default().fg(Color::White));
    match &app.snapshot.mempool {
        Ok(mempool) => {
            let mut text = String::new();
            let _ = writeln!(text, "Transactions:    {}", mempool.transactions);
            let _ = writeln!(text, "Bytes:           {}", human_bytes(mempool.bytes));
            let _ = writeln!(text, "Max mempool:     {}", human_bytes(mempool.maxmempool));
            let _ = writeln!(text, "Min fee:         {:.2} sat/vB", mempool.min_fee_sat_vb);
            let _ = writeln!(text, "Unbroadcast:     {}", mempool.unbroadcast);
            let usage_pct = if mempool.maxmempool > 0 {
                (mempool.bytes as f64 / mempool.maxmempool as f64) * 100.0
            } else {
                0.0
            };
            let _ = write!(text, "Usage:           {:.2}%", usage_pct);
            let para = Paragraph::new(text)
                .block(block)
                .style(Style::default().fg(Color::White))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, area);
        }
        Err(err) => {
            let para = Paragraph::new(format!("Error: {err}"))
                .block(block)
                .style(Style::default().fg(Color::Red))
                .wrap(Wrap { trim: true });
            frame.render_widget(para, area);
        }
    }
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup_area = centered_rect(50, 60, area);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan))
        .title(" Help ")
        .title_style(Style::default().fg(Color::White).add_modifier(Modifier::BOLD));

    let text = "\
Navigation
  Tab / Right     next tab
  Shift+Tab / Left previous tab
  1-4             jump to tab
  Up / Down       scroll peer table

Actions
  r               refresh now
  h / ?           toggle help
  q / Esc / Ctrl-C quit
";
    let paragraph = Paragraph::new(text)
        .block(block)
        .style(Style::default().fg(Color::White))
        .wrap(Wrap { trim: true })
        .alignment(Alignment::Left);
    frame.render_widget(ratatui::widgets::Clear, popup_area);
    frame.render_widget(paragraph, popup_area);
}

fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
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

fn human_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2} GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2} MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.2} kB", bytes as f64 / 1_000.0)
    } else {
        format!("{bytes} B")
    }
}


