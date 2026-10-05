use crate::app::{App, Tab};
use crate::theme::{BLACK, BRIGHT_CYAN, BRIGHT_GREEN, BRIGHT_MAGENTA, BRIGHT_RED, BRIGHT_YELLOW, DARK_BG, ERROR, LIGHT_GRAY, MID_GRAY, MUTED, OK, THEME, WARN, WHITE};
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Margin, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::symbols;
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Cell, Gauge, LineGauge, Paragraph, Row, Scrollbar, ScrollbarOrientation,
    ScrollbarState, Sparkline, StatefulWidget, Table, Tabs, Widget, Wrap,
};
use ratatui::Frame;
use std::fmt::Write as _;
use std::time::Duration;

pub fn render(frame: &mut Frame<'_>, app: &App) {
    frame.render_widget(AppWidget { app }, frame.area());
    if app.show_help {
        render_help(frame, frame.area());
    }
}

struct AppWidget<'a> {
    app: &'a App,
}

impl Widget for AppWidget<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        Block::new().style(THEME.root).render(area, buf);
        let layout =
            Layout::vertical([Constraint::Length(1), Constraint::Min(0), Constraint::Length(1)]);
        let chunks = layout.split(area);
        if chunks.len() < 3 {
            return;
        }
        let title_bar = chunks[0];
        let content = chunks[1];
        let bottom_bar = chunks[2];

        self.render_title_bar(title_bar, buf);
        self.render_content(content, buf);
        render_bottom_bar(bottom_bar, buf);
    }
}

impl AppWidget<'_> {
    fn render_title_bar(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::horizontal([Constraint::Min(0), Constraint::Length(40)]);
        let chunks = layout.split(area);
        let title = chunks[0];
        let tabs_area = chunks[1];

        Span::styled(" rbitcoin-tui ", THEME.app_title).render(title, buf);

        let titles: Vec<Line<'_>> = Tab::ALL
            .iter()
            .map(|t| Line::from(t.title()))
            .collect();
        Tabs::new(titles)
            .style(THEME.tabs)
            .highlight_style(THEME.tabs_selected)
            .select(self.app.tab as usize)
            .divider("")
            .padding("", "")
            .render(tabs_area, buf);
    }

    fn render_content(&self, area: Rect, buf: &mut Buffer) {
        match self.app.tab {
            Tab::Dashboard => self.render_dashboard(area, buf),
            Tab::Chain => self.render_chain(area, buf),
            Tab::Network => self.render_network(area, buf),
            Tab::Mempool => self.render_mempool(area, buf),
            Tab::Console => self.render_console(area, buf),
        }
    }

    fn render_dashboard(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::vertical([
            Constraint::Length(7),
            Constraint::Length(9),
            Constraint::Min(0),
        ])
        .margin(1);
        let chunks = layout.split(area);
        let summary = chunks[0];
        let gauges = chunks[1];
        let bottom = chunks[2];

        self.render_summary_card(summary, buf);
        self.render_gauges_row(gauges, buf);

        let bottom_layout =
            Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)]);
        let bottom_chunks = bottom_layout.split(bottom);
        self.render_mempool_sparkline(bottom_chunks[0], buf);
        self.render_mini_log(bottom_chunks[1], buf);
    }

    fn render_summary_card(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Summary ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        let layout = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
        let chunks = layout.split(inner);
        let left = chunks[0];
        let right = chunks[1];

        let mut left_text = String::new();
        let _ = writeln!(left_text, "version: {}", env!("CARGO_PKG_VERSION"));
        let _ = writeln!(left_text, "endpoint: {}", self.app.snapshot.endpoint_label());
        let _ = writeln!(
            left_text,
            "refresh: every {}s",
            self.app.interval.as_secs()
        );
        let node_status = if self.app.config.has_external_rpc() {
            "external".into()
        } else if let Some(0) = self.app.node_exit_code {
            "stopped".into()
        } else if let Some(code) = self.app.node_exit_code {
            format!("exited {code}")
        } else if self.app.node_child.is_some() {
            if self.app.snapshot.chain.is_ok() || self.app.snapshot.network.is_ok() {
                "running".into()
            } else {
                "starting…".into()
            }
        } else {
            "not started".into()
        };
        let _ = writeln!(left_text, "node: {node_status}");
        if let Some(pid) = self.app.node_pid {
            let _ = writeln!(left_text, "pid: {pid}");
        }
        if let Some(started) = self.app.node_spawned_at {
            let uptime = format_elapsed(started.elapsed());
            let _ = writeln!(left_text, "uptime: {uptime}");
        }
        if let Ok(meta) = std::fs::metadata(&self.app.config.datadir) {
            if meta.is_dir() {
                if let Ok(size) = dir_size(&self.app.config.datadir) {
                    let _ = writeln!(left_text, "datadir: {}", human_bytes(size));
                }
            }
        }
        // Show startup telemetry while the node is starting and RPC isn't up.
        if self.app.node_child.is_some()
            && self.app.node_exit_code.is_none()
            && self.app.snapshot.chain.is_err()
        {
            if let Some(ref op) = self.app.startup_operation {
                let _ = writeln!(left_text, "op: {op}");
            }
            if let Some((cur, tot)) = self.app.startup_progress {
                let _ = writeln!(left_text, "progress: {cur}/{tot}");
            }
            if let Some(h) = self.app.startup_height {
                let _ = writeln!(left_text, "height: {h}");
            }
            if let (Some(i), Some(o)) = (self.app.log_net_in, self.app.log_net_out) {
                let _ = writeln!(left_text, "peers: {i} in / {o} out");
            }
            if let (Some(txs), Some(bytes)) = (self.app.log_mempool_txs, self.app.log_mempool_bytes) {
                let _ = writeln!(left_text, "mempool: {txs} txs / {}", human_bytes(bytes));
            }
            for (k, v) in &self.app.log_store_stats {
                let _ = writeln!(left_text, "store {k}: {v}");
            }
        }
        Paragraph::new(left_text)
            .style(THEME.content)
            .wrap(Wrap { trim: true })
            .render(left, buf);

        let mut right_text = String::new();
        let _ = writeln!(
            right_text,
            "last refresh: {}",
            self.app
                .snapshot
                .refreshed_at
                .map(|t| format_elapsed(t.elapsed()))
                .unwrap_or_else(|| "not yet refreshed".into())
        );
        if let Ok(chain) = &self.app.snapshot.chain {
            let _ = writeln!(right_text, "blocks: {}", chain.blocks);
            let _ = writeln!(right_text, "headers: {}", chain.headers);
        }
        let ready = if self.app.snapshot.is_ready() {
            Span::styled("READY", OK.add_modifier(Modifier::BOLD))
        } else {
            Span::styled("SYNCING", WARN)
        };
        let _ = write!(right_text, "status: ");
        Paragraph::new(right_text)
            .style(THEME.content)
            .wrap(Wrap { trim: true })
            .render(right, buf);
        let ready_line = Line::from(ready);
        let ready_para = Paragraph::new(ready_line).alignment(Alignment::Right);
        ready_para.render(right, buf);

        if let Some(w) = &self.app.snapshot.warnings {
            let warn = Paragraph::new(format!("warning: {w}")).style(WARN);
            warn.render(inner, buf);
        }
        if let Some(err) = &self.app.snapshot.last_error {
            let err_para = Paragraph::new(format!("error: {err}")).style(ERROR);
            err_para.render(inner, buf);
        }
        if let Some(ref w) = self.app.log_last_warn {
            let warn = Paragraph::new(format!("log warn: {w}")).style(WARN);
            warn.render(inner, buf);
        }
        if let Some(ref e) = self.app.log_last_error {
            let err = Paragraph::new(format!("log error: {e}")).style(ERROR);
            err.render(inner, buf);
        }
    }

    fn render_gauges_row(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)]);
        let chunks = layout.split(area);
        let left = chunks[0];
        let right = chunks[1];

        let block = Block::bordered()
            .title(" Verification Progress ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(left);
        block.render(left, buf);

        match &self.app.snapshot.chain {
            Ok(chain) => {
                let ratio = chain.verification_progress.clamp(0.0, 1.0);
                let color = if ratio >= 0.99 {
                    BRIGHT_GREEN
                } else if ratio >= 0.5 {
                    BRIGHT_YELLOW
                } else {
                    BRIGHT_RED
                };
                let label = format!("{:.2}%", ratio * 100.0);
                let bar_width = inner.width.saturating_sub(2) as usize;
                let bar = block_bar(ratio, bar_width.max(1));
                let bar_line = Line::from(vec![
                    Span::styled(bar, Style::new().fg(color).add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {label}"), Style::new().fg(WHITE)),
                ]);
                let bar_para = Paragraph::new(bar_line).alignment(Alignment::Center);
                bar_para.render(inner, buf);

                self.render_dashboard_network(right, buf, Some(chain));
            }
            Err(_) => {
                let bar_width = inner.width.saturating_sub(2) as usize;
                let bar = block_bar(0.0, bar_width.max(1));
                let bar_line = Line::from(vec![
                    Span::styled(bar, Style::new().fg(MID_GRAY)),
                    Span::styled("  starting…", Style::new().fg(WHITE)),
                ]);
                let bar_para = Paragraph::new(bar_line).alignment(Alignment::Center);
                bar_para.render(inner, buf);

                self.render_dashboard_network(right, buf, None);
            }
        }
    }

    fn render_dashboard_network(&self, area: Rect, buf: &mut Buffer, chain: Option<&crate::rpc::BlockchainInfo>) {
        let net_block = Block::bordered()
            .title(" Network ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let net_inner = net_block.inner(area);
        net_block.render(area, buf);

        let mut net_text = String::new();
        let _ = writeln!(
            net_text,
            "in: {}  out: {}",
            self.app
                .snapshot
                .network
                .as_ref()
                .map(|n| n.connections_in)
                .unwrap_or(0),
            self.app
                .snapshot
                .network
                .as_ref()
                .map(|n| n.connections_out)
                .unwrap_or(0)
        );
        let _ = writeln!(
            net_text,
            "time offset: {}s",
            self.app
                .snapshot
                .network
                .as_ref()
                .map(|n| n.timeoffset)
                .unwrap_or(0)
        );
        if let Some(net) = self.app.snapshot.network.as_ref().ok() {
            let _ = writeln!(net_text, "relay fee: {:.5} BTC/kvB", net.relayfee);
        }
        if let Some(chain) = chain {
            let _ = writeln!(net_text, "chain: {}", chain.chain);
            let _ = writeln!(net_text, "blocks: {}", chain.blocks);
            let _ = write!(net_text, "headers: {}", chain.headers);
        } else {
            let _ = write!(net_text, "waiting for chain data…");
        }
        Paragraph::new(net_text)
            .style(THEME.content)
            .wrap(Wrap { trim: true })
            .render(net_inner, buf);
    }

    fn render_mempool_sparkline(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Mempool ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        let layout = Layout::horizontal([Constraint::Percentage(60), Constraint::Percentage(40)]);
        let chunks = layout.split(inner);
        let chart_area = chunks[0];
        let stats_area = chunks[1];

        let data: Vec<u64> = self.app.mempool_tx_history.iter().copied().collect();
        if data.len() >= 2 {
            let sparkline = Sparkline::default()
                .data(&data)
                .style(THEME.sparkline.data)
                .max(*data.iter().max().unwrap_or(&1).max(&1))
                .bar_set(symbols::bar::NINE_LEVELS);
            sparkline.render(chart_area, buf);
        } else {
            let placeholder = Paragraph::new("collecting data…")
                .style(MUTED)
                .alignment(Alignment::Center);
            placeholder.render(chart_area, buf);
        }

        let mut text = String::new();
        match &self.app.snapshot.mempool {
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
        Paragraph::new(text)
            .style(THEME.content)
            .alignment(Alignment::Right)
            .wrap(Wrap { trim: true })
            .render(stats_area, buf);
    }

    fn render_mini_log(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Latest Logs ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        let visible = inner.height as usize;
        let lines: Vec<Line<'_>> = self
            .app
            .console_lines
            .iter()
            .rev()
            .take(visible)
            .rev()
            .map(|line| console_line(line))
            .collect();
        Paragraph::new(lines).render(inner, buf);
    }

    fn render_chain(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::vertical([Constraint::Length(8), Constraint::Min(0)]).margin(1);
        let chunks = layout.split(area);
        let gauge_area = chunks[0];
        let info_area = chunks[1];

        let outer = Block::bordered()
            .border_style(THEME.borders)
            .title(" Chain ")
            .title_style(THEME.app_title)
            .style(Style::new().bg(BLACK));
        outer.render(area, buf);

        match &self.app.snapshot.chain {
            Ok(chain) => {
                let ratio = chain.verification_progress.clamp(0.0, 1.0);
                let color = if ratio >= 0.99 {
                    BRIGHT_GREEN
                } else if ratio >= 0.5 {
                    BRIGHT_YELLOW
                } else {
                    BRIGHT_RED
                };
                let label = format!("{:.4}%", ratio * 100.0);
                let bar_width = gauge_area.width.saturating_sub(4) as usize;
                let bar = block_bar(ratio, bar_width.max(1));

                let gauge_block = Block::bordered()
                    .title(" Sync Progress ")
                    .title_style(THEME.description_title)
                    .border_style(THEME.borders)
                    .style(Style::new().bg(BLACK));
                let gauge_inner = gauge_block.inner(gauge_area);
                gauge_block.render(gauge_area, buf);

                let bar_line = Line::from(vec![
                    Span::styled(bar, Style::new().fg(color).add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {label}"), Style::new().fg(WHITE)),
                ]);
                Paragraph::new(bar_line)
                    .alignment(Alignment::Center)
                    .render(gauge_inner, buf);

                let info_layout = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
                let info_chunks = info_layout.split(info_area);
                let left = info_chunks[0];
                let right = info_chunks[1];

                let mut left_text = String::new();
                let _ = writeln!(left_text, "Chain:        {}", chain.chain);
                let _ = writeln!(left_text, "Blocks:       {}", chain.blocks);
                let _ = writeln!(left_text, "Headers:      {}", chain.headers);
                let _ = writeln!(left_text, "IBD:          {}", if chain.initial_block_download { "yes" } else { "no" });
                let _ = writeln!(left_text, "Pruned:       {}", if chain.pruned { "yes" } else { "no" });
                let _ = writeln!(left_text, "Size on disk: {}", human_bytes(chain.size_on_disk));
                let _ = writeln!(left_text, "Median time:  {}", chain.mediantime);
                Paragraph::new(left_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(left, buf);

                let mut right_text = String::new();
                let hash_short = if chain.bestblockhash.len() > 16 {
                    format!("{}…{}", &chain.bestblockhash[..8], &chain.bestblockhash[chain.bestblockhash.len()-8..])
                } else {
                    chain.bestblockhash.clone()
                };
                let work_short = if chain.chainwork.len() > 16 {
                    format!("{}…{}", &chain.chainwork[..8], &chain.chainwork[chain.chainwork.len()-8..])
                } else {
                    chain.chainwork.clone()
                };
                let _ = writeln!(right_text, "Best block:   {}", hash_short);
                let _ = writeln!(right_text, "Difficulty:   {:.4}", chain.difficulty);
                let _ = writeln!(right_text, "Chain work:   {}", work_short);
                let _ = writeln!(right_text, "Progress:     {:.4}%", chain.verification_progress * 100.0);
                let _ = writeln!(right_text, "Remaining:    {}", chain.headers.saturating_sub(chain.blocks));
                Paragraph::new(right_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(right, buf);
            }
            Err(ref e) => {
                let (ratio, label) = if let Some((cur, tot)) = self.app.startup_progress {
                    let r = (cur as f64 / tot as f64).clamp(0.0, 1.0);
                    (r, format!("{:.1}%  {cur}/{tot}", r * 100.0))
                } else {
                    (0.0, "starting…".into())
                };
                let bar_width = gauge_area.width.saturating_sub(4) as usize;
                let bar = block_bar(ratio, bar_width.max(1));

                let gauge_block = Block::bordered()
                    .title(" Sync Progress ")
                    .title_style(THEME.description_title)
                    .border_style(THEME.borders)
                    .style(Style::new().bg(BLACK));
                let gauge_inner = gauge_block.inner(gauge_area);
                gauge_block.render(gauge_area, buf);

                let bar_line = Line::from(vec![
                    Span::styled(bar, Style::new().fg(BRIGHT_CYAN).add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {label}"), Style::new().fg(WHITE)),
                ]);
                Paragraph::new(bar_line)
                    .alignment(Alignment::Center)
                    .render(gauge_inner, buf);

                let mut rows = Vec::new();
                if e != "no RPC endpoint responded" {
                    rows.push(format!("Error: {e}"));
                } else {
                    if let Some(ref op) = self.app.startup_operation {
                        rows.push(format!("operation: {op}"));
                    }
                    if let Some((cur, tot)) = self.app.startup_progress {
                        let pct = (cur as f64 / tot as f64) * 100.0;
                        rows.push(format!("progress: {cur}/{tot} ({pct:.1}%)"));
                    }
                    if let Some(h) = self.app.startup_height {
                        rows.push(format!("height: {h}"));
                    }
                    if rows.is_empty() {
                        rows.push("node is initializing…".into());
                    }
                }
                let style = if e == "no RPC endpoint responded" { MUTED } else { ERROR };
                let placeholder = Paragraph::new(rows.join("\n"))
                    .style(style)
                    .alignment(Alignment::Center);
                placeholder.render(info_area, buf);
            }
        }
    }

    fn render_network(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::vertical([
            Constraint::Length(8),
            Constraint::Length(10),
            Constraint::Min(0),
        ])
        .margin(1);
        let chunks = layout.split(area);
        let summary = chunks[0];
        let map_area = chunks[1];
        let peers = chunks[2];

        let outer = Block::bordered()
            .border_style(THEME.borders)
            .title(" Network ")
            .title_style(THEME.app_title)
            .style(Style::new().bg(BLACK));
        outer.render(area, buf);

        self.render_net_summary(summary, buf);
        self.render_peer_map(map_area, buf);
        self.render_peer_table(peers, buf);
    }

    fn render_net_summary(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Connections ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        match &self.app.snapshot.network {
            Ok(net) => {
                let layout = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
                let chunks = layout.split(inner);
                let left = chunks[0];
                let right = chunks[1];

                let mut left_text = String::new();
                let _ = writeln!(left_text, "Inbound:      {}", net.connections_in);
                let _ = writeln!(left_text, "Outbound:     {}", net.connections_out);
                let _ = writeln!(left_text, "Time offset:  {}s", net.timeoffset);
                let _ = writeln!(left_text, "Relay fee:    {:.5} BTC/kvB", net.relayfee);
                let _ = writeln!(left_text, "Network:      {}", if net.networkactive { "active" } else { "inactive" });
                Paragraph::new(left_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(left, buf);

                let mut right_text = String::new();
                let _ = writeln!(right_text, "Version:      {}", net.version);
                let _ = writeln!(right_text, "Subversion:   {}", net.subversion);
                let _ = writeln!(right_text, "Protocol:     {}", net.protocolversion);
                if !net.localaddresses.is_empty() {
                    let addrs = net.localaddresses.join(", ");
                    let _ = writeln!(right_text, "Local addrs:  {}", addrs);
                }
                if let Some(w) = &net.warnings {
                    let _ = writeln!(right_text, "Warnings:     {w}");
                }
                Paragraph::new(right_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(right, buf);
            }
            Err(ref e) if e == "no RPC endpoint responded" => {
                let mut text = String::new();
                if let Some(ref op) = self.app.startup_operation {
                    let _ = writeln!(text, "node is initializing: {op}");
                }
                if let Some((cur, tot)) = self.app.startup_progress {
                    let _ = writeln!(text, "progress: {cur}/{tot}");
                }
                if let Some(h) = self.app.startup_height {
                    let _ = writeln!(text, "height: {h}");
                }
                if let Some(i) = self.app.log_net_in {
                    let _ = writeln!(text, "Inbound:     {i} (from logs)");
                }
                if let Some(o) = self.app.log_net_out {
                    let _ = writeln!(text, "Outbound:    {o} (from logs)");
                }
                if text.is_empty() {
                    text = "waiting for network data…".into();
                }
                Paragraph::new(text)
                    .style(MUTED)
                    .wrap(Wrap { trim: true })
                    .render(inner, buf);
            }
            Err(ref e) => {
                Paragraph::new(format!("Error: {e}"))
                    .style(ERROR)
                    .wrap(Wrap { trim: true })
                    .render(inner, buf);
            }
        }
    }

    fn render_peer_table(&self, area: Rect, buf: &mut Buffer) {
        let header = Row::new(vec![
            "ID", "Address", "Type", "Height", "Ping", "↓ Bytes", "↑ Bytes", "Ban", "Version", "Sync",
        ])
        .style(THEME.table.header)
        .height(1);

        match &self.app.snapshot.peers {
            Ok(peers) => {
                let rows: Vec<Row<'_>> = peers
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let ping = p
                            .ping_ms
                            .map(|ms| format!("{ms}ms"))
                            .unwrap_or_else(|| "-".into());
                        let ping_color = match p.ping_ms {
                            None => MID_GRAY,
                            Some(ms) if ms < 50 => BRIGHT_GREEN,
                            Some(ms) if ms < 150 => BRIGHT_YELLOW,
                            Some(_) => BRIGHT_RED,
                        };
                        let arrow = if p.inbound { "↓ " } else { "↑ " };
                        let conn_color = if p.inbound { BRIGHT_CYAN } else { BRIGHT_GREEN };
                        let sync = if p.synched_headers < 0 {
                            "-".into()
                        } else {
                            format!("{}/{}", p.synched_headers, p.synched_blocks)
                        };
                        let style = if i % 2 == 0 {
                            THEME.table.row
                        } else {
                            THEME.table.row_alt
                        };
                        Row::new(vec![
                            Cell::from(p.id.to_string()),
                            Cell::from(p.addr.clone()),
                            Cell::from(Line::from(vec![
                                Span::styled(arrow, Style::new().fg(conn_color).add_modifier(Modifier::BOLD)),
                                Span::styled(p.conn_type.clone(), Style::new().fg(conn_color)),
                            ])),
                            Cell::from(p.height.to_string()),
                            Cell::from(ping).style(Style::new().fg(ping_color)),
                            Cell::from(human_bytes(p.bytesrecv)),
                            Cell::from(human_bytes(p.bytessent)),
                            Cell::from(p.banscore.to_string()),
                            Cell::from(p.subver.clone()),
                            Cell::from(sync),
                        ])
                        .height(1)
                        .style(style)
                    })
                    .collect();

                let visible = area.height.saturating_sub(3) as usize;
                let max_scroll = peers.len().saturating_sub(visible);
                let scroll = self.app.peer_table_scroll.min(max_scroll);

                let table = Table::new(
                    rows,
                    [
                        Constraint::Length(5),
                        Constraint::Min(18),
                        Constraint::Length(10),
                        Constraint::Length(8),
                        Constraint::Length(8),
                        Constraint::Length(9),
                        Constraint::Length(9),
                        Constraint::Length(5),
                        Constraint::Min(12),
                        Constraint::Length(10),
                    ],
                )
                .header(header)
                .block(
                    Block::bordered()
                        .title(format!(" Peers ({} connected) ", peers.len()))
                        .title_style(THEME.description_title)
                        .border_style(THEME.borders)
                        .style(Style::new().bg(BLACK)),
                )
                .row_highlight_style(THEME.table.selected);

                let mut state = ratatui::widgets::TableState::default();
                if !peers.is_empty() {
                    state.select(Some(scroll));
                }
                ratatui::widgets::Widget::render(table, area, buf);

                if peers.len() > visible {
                    let scrollbar = Scrollbar::default()
                        .orientation(ScrollbarOrientation::VerticalRight);
                    let mut sb_state = ScrollbarState::new(peers.len()).position(scroll);
                    let sb_area = area.inner(Margin {
                        horizontal: 0,
                        vertical: 1,
                    });
                    scrollbar.render(sb_area, buf, &mut sb_state);
                }
            }
            Err(ref e) if e == "no RPC endpoint responded" => {
                if self.app.log_peer_events.is_empty() {
                    let mut msg = String::new();
                    if let Some(ref op) = self.app.startup_operation {
                        let _ = writeln!(msg, "node is initializing: {op}");
                    }
                    if let Some((cur, tot)) = self.app.startup_progress {
                        let _ = writeln!(msg, "progress: {cur}/{tot}");
                    }
                    if let Some(h) = self.app.startup_height {
                        let _ = writeln!(msg, "height: {h}");
                    }
                    if msg.is_empty() {
                        msg = "waiting for peer data…".into();
                    }
                    Paragraph::new(msg)
                        .block(
                            Block::bordered()
                                .title(" Peers ")
                                .title_style(THEME.description_title)
                                .border_style(THEME.borders)
                                .style(Style::new().bg(BLACK)),
                        )
                        .style(MUTED)
                        .wrap(Wrap { trim: true })
                        .render(area, buf);
                } else {
                    let header = Row::new(vec!["Event"])
                        .style(THEME.table.header)
                        .height(1);
                    let visible = area.height.saturating_sub(3) as usize;
                    let rows: Vec<Row<'_>> = self
                        .app
                        .log_peer_events
                        .iter()
                        .rev()
                        .take(visible)
                        .rev()
                        .map(|line| {
                            let color = if line.contains("disconnect") || line.contains("drop") || line.contains("ban") {
                                BRIGHT_RED
                            } else if line.contains("connect") || line.contains("handshake") {
                                BRIGHT_GREEN
                            } else {
                                THEME.content.fg.unwrap_or(LIGHT_GRAY)
                            };
                            Row::new(vec![Cell::from(line.as_str()).style(Style::new().fg(color))])
                                .height(1)
                        })
                        .collect();
                    let table = Table::new(rows, [Constraint::Min(0)])
                        .header(header)
                        .block(
                            Block::bordered()
                                .title(" Peers (from logs) ")
                                .title_style(THEME.description_title)
                                .border_style(THEME.borders)
                                .style(Style::new().bg(BLACK)),
                        );
                    ratatui::widgets::Widget::render(table, area, buf);
                }
            }
            Err(ref e) => {
                Paragraph::new(format!("Error: {e}"))
                    .block(
                        Block::bordered()
                            .title(" Peers ")
                            .title_style(THEME.description_title)
                            .border_style(THEME.borders)
                            .style(Style::new().bg(BLACK)),
                    )
                    .style(ERROR)
                    .wrap(Wrap { trim: true })
                    .render(area, buf);
            }
        }
    }

    fn render_peer_map(&self, area: Rect, buf: &mut Buffer) {
        use ratatui::widgets::canvas::Canvas;

        let block = Block::bordered()
            .title(" Peer Map ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        if inner.width < 10 || inner.height < 4 {
            return;
        }

        let peers = match &self.app.snapshot.peers {
            Ok(p) if !p.is_empty() => p.clone(),
            _ => {
                let placeholder = Paragraph::new("no peers yet…")
                    .style(MUTED)
                    .alignment(Alignment::Center);
                placeholder.render(inner, buf);
                return;
            }
        };

        let w = inner.width as f64;
        let h = inner.height as f64;
        let cx = w / 2.0;
        let cy = h / 2.0;
        let radius = (w.min(h) / 2.0) * 0.75;

        Canvas::default()
            .x_bounds([0.0, w])
            .y_bounds([0.0, h])
            .paint(|ctx| {
                // Draw our node in the center
                ctx.print(cx, cy, Span::styled("◉", Style::default().fg(BRIGHT_YELLOW)));

                // Draw peers in a circle
                let n = peers.len().max(1) as f64;
                for (i, peer) in peers.iter().enumerate() {
                    let angle = (i as f64 / n) * 2.0 * std::f64::consts::PI;
                    let px = cx + radius * angle.cos();
                    let py = cy + radius * angle.sin() * 0.6; // flatten for terminal aspect ratio
                    let symbol = if peer.inbound { "◆" } else { "●" };
                    let color = if peer.inbound { BRIGHT_CYAN } else { BRIGHT_GREEN };
                    ctx.print(px, py, Span::styled(symbol, Style::default().fg(color)));
                }
            })
            .render(inner, buf);
    }

    fn render_mempool(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::vertical([Constraint::Length(10), Constraint::Length(8), Constraint::Min(0)]).margin(1);
        let chunks = layout.split(area);
        let charts = chunks[0];
        let usage = chunks[1];
        let details = chunks[2];

        let outer = Block::bordered()
            .border_style(THEME.borders)
            .title(" Mempool ")
            .title_style(THEME.app_title)
            .style(Style::new().bg(BLACK));
        outer.render(area, buf);

        self.render_mempool_charts(charts, buf);
        self.render_mempool_usage(usage, buf);
        self.render_mempool_details(details, buf);
    }

    fn render_mempool_charts(&self, area: Rect, buf: &mut Buffer) {
        let layout = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
        let chunks = layout.split(area);
        let fee_area = chunks[0];
        let tx_area = chunks[1];

        let fee_data: Vec<u64> = self
            .app
            .mempool_fee_history
            .iter()
            .map(|f| (*f).clamp(0.0, u64::MAX as f64) as u64)
            .collect();

        let fee_block = Block::bordered()
            .title(" Min Fee History (sat/vB) ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let fee_inner = fee_block.inner(fee_area);
        fee_block.render(fee_area, buf);
        if fee_data.len() >= 2 {
            let sparkline = Sparkline::default()
                .data(&fee_data)
                .style(Style::new().fg(BRIGHT_MAGENTA))
                .max(*fee_data.iter().max().unwrap_or(&1).max(&1))
                .bar_set(symbols::bar::NINE_LEVELS);
            sparkline.render(fee_inner, buf);
        } else {
            Paragraph::new("collecting data…")
                .style(MUTED)
                .alignment(Alignment::Center)
                .render(fee_inner, buf);
        }

        let tx_data: Vec<u64> = self.app.mempool_tx_history.iter().copied().collect();
        let tx_block = Block::bordered()
            .title(" Tx Count History ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let tx_inner = tx_block.inner(tx_area);
        tx_block.render(tx_area, buf);
        if tx_data.len() >= 2 {
            let sparkline = Sparkline::default()
                .data(&tx_data)
                .style(THEME.sparkline.data)
                .max(*tx_data.iter().max().unwrap_or(&1).max(&1))
                .bar_set(symbols::bar::NINE_LEVELS);
            sparkline.render(tx_inner, buf);
        } else {
            Paragraph::new("collecting data…")
                .style(MUTED)
                .alignment(Alignment::Center)
                .render(tx_inner, buf);
        }
    }

    fn render_mempool_usage(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Memory Usage ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        match &self.app.snapshot.mempool {
            Ok(mempool) => {
                let usage_pct = if mempool.maxmempool > 0 {
                    (mempool.bytes as f64 / mempool.maxmempool as f64)
                } else {
                    0.0
                };
                let bar_width = inner.width.saturating_sub(4) as usize;
                let bar = block_bar(usage_pct, bar_width.max(1));
                let color = if usage_pct >= 0.9 {
                    BRIGHT_RED
                } else if usage_pct >= 0.5 {
                    BRIGHT_YELLOW
                } else {
                    BRIGHT_GREEN
                };
                let bar_line = Line::from(vec![
                    Span::styled(bar, Style::new().fg(color).add_modifier(Modifier::BOLD)),
                    Span::styled(format!("  {:.2}%  {}/{}", usage_pct * 100.0, human_bytes(mempool.bytes), human_bytes(mempool.maxmempool)), Style::new().fg(WHITE)),
                ]);
                Paragraph::new(bar_line)
                    .alignment(Alignment::Center)
                    .render(inner, buf);
            }
            Err(ref e) if e == "no RPC endpoint responded" => {
                let mut text = String::new();
                if let Some(txs) = self.app.log_mempool_txs {
                    let _ = writeln!(text, "Transactions: {txs} (from logs)");
                }
                if let Some(bytes) = self.app.log_mempool_bytes {
                    let _ = writeln!(text, "Bytes: {} (from logs)", human_bytes(bytes));
                }
                if text.is_empty() {
                    text = "waiting for mempool data…".into();
                }
                Paragraph::new(text)
                    .style(MUTED)
                    .alignment(Alignment::Center)
                    .render(inner, buf);
            }
            Err(ref e) => {
                Paragraph::new(format!("Error: {e}"))
                    .style(ERROR)
                    .alignment(Alignment::Center)
                    .render(inner, buf);
            }
        }
    }

    fn render_mempool_details(&self, area: Rect, buf: &mut Buffer) {
        let block = Block::bordered()
            .title(" Details ")
            .title_style(THEME.description_title)
            .border_style(THEME.borders)
            .style(Style::new().bg(BLACK));
        let inner = block.inner(area);
        block.render(area, buf);

        match &self.app.snapshot.mempool {
            Ok(mempool) => {
                let layout = Layout::horizontal([Constraint::Percentage(50), Constraint::Percentage(50)]);
                let chunks = layout.split(inner);
                let left = chunks[0];
                let right = chunks[1];

                let mut left_text = String::new();
                let _ = writeln!(left_text, "Transactions:    {}", mempool.transactions);
                let _ = writeln!(left_text, "Bytes:           {}", human_bytes(mempool.bytes));
                let _ = writeln!(left_text, "Usage:           {}", human_bytes(mempool.usage));
                let _ = writeln!(left_text, "Max mempool:     {}", human_bytes(mempool.maxmempool));
                let _ = writeln!(left_text, "Min fee:         {:.2} sat/vB", mempool.min_fee_sat_vb);
                Paragraph::new(left_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(left, buf);

                let mut right_text = String::new();
                let _ = writeln!(right_text, "Total fee:       {:.8} BTC", mempool.total_fee);
                let _ = writeln!(right_text, "Unbroadcast:     {}", mempool.unbroadcast);
                let _ = writeln!(right_text, "Ancestor limit:  {}", mempool.ancestorlimit);
                let _ = writeln!(right_text, "Descendant limit:{}", mempool.descendantlimit);
                Paragraph::new(right_text)
                    .style(THEME.content)
                    .wrap(Wrap { trim: true })
                    .render(right, buf);
            }
            Err(ref e) if e == "no RPC endpoint responded" => {
                let mut text = String::new();
                if let Some(ref op) = self.app.startup_operation {
                    let _ = writeln!(text, "node is initializing: {op}");
                }
                if let Some((cur, tot)) = self.app.startup_progress {
                    let _ = writeln!(text, "progress: {cur}/{tot}");
                }
                if let Some(h) = self.app.startup_height {
                    let _ = writeln!(text, "height: {h}");
                }
                if let Some(txs) = self.app.log_mempool_txs {
                    let _ = writeln!(text, "Transactions:    {txs} (from logs)");
                }
                if let Some(bytes) = self.app.log_mempool_bytes {
                    let _ = writeln!(text, "Bytes:           {} (from logs)", human_bytes(bytes));
                }
                if text.is_empty() {
                    text = "waiting for mempool data…".into();
                }
                Paragraph::new(text)
                    .style(MUTED)
                    .wrap(Wrap { trim: true })
                    .render(inner, buf);
            }
            Err(ref e) => {
                Paragraph::new(format!("Error: {e}"))
                    .style(ERROR)
                    .wrap(Wrap { trim: true })
                    .render(inner, buf);
            }
        }
    }

    fn render_console(&self, area: Rect, buf: &mut Buffer) {
        let outer = Block::bordered()
            .border_style(THEME.borders)
            .title(" Console ")
            .title_style(THEME.app_title);
        let inner = outer.inner(area);
        outer.render(area, buf);

        let layout = Layout::vertical([Constraint::Min(0), Constraint::Length(1)]);
        let chunks = layout.split(inner);
        let log_area = chunks[0];
        let prompt_area = chunks[1];

        if self.app.console_lines.is_empty() {
            let msg = if self.app.config.has_external_rpc() {
                "no local logs — connected to external node"
            } else {
                "no log output yet — node is starting up…"
            };
            let placeholder = Paragraph::new(msg)
                .style(MUTED)
                .alignment(Alignment::Center);
            placeholder.render(log_area, buf);
        } else {
            let visible = log_area.height as usize;
            let max_scroll = self.app.console_lines.len().saturating_sub(visible);
            let scroll = self.app.console_scroll.min(max_scroll);

            let lines: Vec<Line<'_>> = self
                .app
                .console_lines
                .iter()
                .skip(scroll)
                .take(visible)
                .map(|line| console_line(line))
                .collect();

            Paragraph::new(lines).render(log_area, buf);

            if self.app.console_lines.len() > visible {
                let scrollbar = Scrollbar::default()
                    .orientation(ScrollbarOrientation::VerticalRight);
                let mut sb_state = ScrollbarState::new(self.app.console_lines.len()).position(scroll);
                let sb_area = log_area.inner(Margin {
                    horizontal: 0,
                    vertical: 0,
                });
                StatefulWidget::render(scrollbar, sb_area, buf, &mut sb_state);
            }
        }

        let prompt = format!("> {}", self.app.command_input);
        let prompt_para = Paragraph::new(prompt)
            .style(Style::new().fg(BRIGHT_CYAN).add_modifier(Modifier::BOLD));
        prompt_para.render(prompt_area, buf);
    }
}

fn console_line(line: &str) -> Line<'_> {
    // Level-based colors take priority for errors/warnings.
    let style = if line.contains(" ERROR ") {
        ERROR
    } else if line.contains(" WARN ") {
        WARN
    } else if line.contains(" DEBUG ") || line.contains(" TRACE ") {
        MUTED
    } else {
        // Module-based tint for INFO lines.
        if line.contains(" store:") || line.contains(" store::") {
            Style::new().fg(BRIGHT_GREEN)
        } else if line.contains(" net:") || line.contains(" net::") {
            Style::new().fg(BRIGHT_CYAN)
        } else if line.contains(" mempool:") || line.contains(" mempool::") {
            Style::new().fg(BRIGHT_MAGENTA)
        } else if line.contains(" chain:") || line.contains(" chain::") {
            Style::new().fg(BRIGHT_YELLOW)
        } else if line.contains(" node:") || line.contains(" node::") {
            Style::new().fg(WHITE)
        } else {
            THEME.content
        }
    };
    Line::from(Span::styled(line.to_string(), style))
}

/// Recursively calculate directory size.
fn dir_size(path: &std::path::Path) -> Result<u64, std::io::Error> {
    let mut total = 0;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        if meta.is_dir() {
            total += dir_size(&entry.path())?;
        } else {
            total += meta.len();
        }
    }
    Ok(total)
}

fn render_bottom_bar(area: Rect, buf: &mut Buffer) {
    let keys = [
        ("Tab/→", "Next"),
        ("Shift+Tab/←", "Prev"),
        ("1-5", "Tab"),
        ("R", "Refresh"),
        ("H/?", "Help"),
        ("Q/Esc", "Quit"),
    ];
    let spans: Vec<Span<'_>> = keys
        .iter()
        .flat_map(|(key, desc)| {
            [
                Span::styled(format!(" {key} "), THEME.key_binding.key),
                Span::styled(format!(" {desc} "), THEME.key_binding.description),
            ]
        })
        .collect();
    Line::from(spans)
        .centered()
        .style(Style::new().fg(MID_GRAY).bg(BLACK))
        .render(area, buf);
}

fn render_help(frame: &mut Frame<'_>, area: Rect) {
    let popup_area = centered_rect(50, 60, area);
    let block = Block::bordered()
        .border_style(Style::new().fg(BRIGHT_CYAN))
        .title(" Help ")
        .title_style(Style::new().fg(WHITE).add_modifier(Modifier::BOLD));

    let text = "\
Navigation
  Tab / Right       next tab
  Shift+Tab / Left  previous tab
  1-5               jump to tab
  Up / Down         scroll peer table / console

Console (type RPC commands)
  Type              enter command
  Enter             execute command
  Backspace         delete character

Actions
  r                 refresh now
  h / ?             toggle help
  q / Esc / Ctrl-C  quit
";
    let paragraph = Paragraph::new(text)
        .block(block)
        .style(Style::new().fg(WHITE))
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

fn block_bar(ratio: f64, width: usize) -> String {
    let filled = (ratio * width as f64).clamp(0.0, width as f64) as usize;
    let empty = width.saturating_sub(filled);
    format!("{}{}", "█".repeat(filled), "░".repeat(empty))
}

fn text_bar(value: u64, max: u64, width: usize, color: ratatui::style::Color) -> Line<'static> {
    let ratio = if max > 0 {
        value as f64 / max as f64
    } else {
        0.0
    };
    let filled = (ratio * width as f64).clamp(0.0, width as f64) as usize;
    let empty = width.saturating_sub(filled);
    let bar = format!("{}{}", "█".repeat(filled), "░".repeat(empty));
    Line::from(vec![
        Span::styled(bar, Style::new().fg(color)),
        Span::styled(format!(" {:.1}%", ratio * 100.0), Style::new().fg(LIGHT_GRAY)),
    ])
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


