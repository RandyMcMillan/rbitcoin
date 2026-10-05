use ratatui::style::{Color, Modifier, Style};

pub struct Theme {
    pub root: Style,
    pub content: Style,
    pub app_title: Style,
    pub tabs: Style,
    pub tabs_selected: Style,
    pub borders: Style,
    pub description: Style,
    pub description_title: Style,
    pub key_binding: KeyBinding,
    pub gauge: Gauge,
    pub chart: Chart,
    pub table: Table,
    pub sparkline: Sparkline,
    pub barchart: BarChart,
}

pub struct KeyBinding {
    pub key: Style,
    pub description: Style,
}

pub struct Gauge {
    pub filled: Style,
    pub unfilled: Style,
    pub label: Style,
}

pub struct Chart {
    pub axis: Style,
    pub dataset: Style,
    pub grid: Style,
}

pub struct Table {
    pub header: Style,
    pub row: Style,
    pub row_alt: Style,
    pub selected: Style,
}

pub struct Sparkline {
    pub data: Style,
}

pub struct BarChart {
    pub bar: Style,
    pub value: Style,
    pub label: Style,
}

pub const THEME: Theme = Theme {
    root: Style::new().bg(DARK_BLUE),
    content: Style::new().bg(DARK_BLUE).fg(LIGHT_GRAY),
    app_title: Style::new()
        .fg(WHITE)
        .bg(DARK_BLUE)
        .add_modifier(Modifier::BOLD),
    tabs: Style::new().fg(MID_GRAY).bg(DARK_BLUE),
    tabs_selected: Style::new()
        .fg(WHITE)
        .bg(DARK_BLUE)
        .add_modifier(Modifier::BOLD)
        .add_modifier(Modifier::REVERSED),
    borders: Style::new().fg(LIGHT_GRAY),
    description: Style::new().fg(LIGHT_GRAY).bg(DARK_BLUE),
    description_title: Style::new()
        .fg(LIGHT_GRAY)
        .add_modifier(Modifier::BOLD),
    key_binding: KeyBinding {
        key: Style::new().fg(BLACK).bg(DARK_GRAY),
        description: Style::new().fg(DARK_GRAY).bg(BLACK),
    },
    gauge: Gauge {
        filled: Style::new().fg(LIGHT_BLUE).bg(DARK_BLUE),
        unfilled: Style::new().fg(DARK_GRAY).bg(DARK_BLUE),
        label: Style::new().fg(WHITE).add_modifier(Modifier::BOLD),
    },
    chart: Chart {
        axis: Style::new().fg(MID_GRAY),
        dataset: Style::new().fg(LIGHT_GREEN),
        grid: Style::new().fg(DARK_GRAY),
    },
    table: Table {
        header: Style::new()
            .fg(YELLOW)
            .add_modifier(Modifier::BOLD),
        row: Style::new().fg(LIGHT_GRAY),
        row_alt: Style::new().fg(LIGHT_GRAY).bg(DARKER_BLUE),
        selected: Style::new()
            .fg(BLACK)
            .bg(LIGHT_YELLOW)
            .add_modifier(Modifier::BOLD),
    },
    sparkline: Sparkline {
        data: Style::new().fg(CYAN),
    },
    barchart: BarChart {
        bar: Style::new().fg(LIGHT_BLUE),
        value: Style::new().fg(BLACK).bg(LIGHT_BLUE),
        label: Style::new().fg(LIGHT_GRAY),
    },
};

const DARK_BLUE: Color = Color::Rgb(16, 24, 48);
const DARKER_BLUE: Color = Color::Rgb(12, 18, 36);
const LIGHT_BLUE: Color = Color::Rgb(64, 128, 224);
const CYAN: Color = Color::Rgb(64, 192, 224);
const YELLOW: Color = Color::Rgb(224, 192, 64);
const LIGHT_YELLOW: Color = Color::Rgb(255, 224, 128);
const LIGHT_GREEN: Color = Color::Rgb(64, 224, 128);
const RED: Color = Color::Rgb(224, 64, 64);
const LIGHT_RED: Color = Color::Rgb(255, 96, 96);
const BLACK: Color = Color::Rgb(8, 8, 8);
const DARK_GRAY: Color = Color::Rgb(68, 68, 68);
const MID_GRAY: Color = Color::Rgb(128, 128, 128);
const LIGHT_GRAY: Color = Color::Rgb(188, 188, 188);
const WHITE: Color = Color::Rgb(238, 238, 238);
