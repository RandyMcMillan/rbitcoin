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
    pub table: Table,
    pub sparkline: Sparkline,
}

pub struct KeyBinding {
    pub key: Style,
    pub description: Style,
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

pub const THEME: Theme = Theme {
    root: Style::new().bg(BLACK),
    content: Style::new().fg(LIGHT_GRAY),
    app_title: Style::new().fg(WHITE).add_modifier(Modifier::BOLD),
    tabs: Style::new().fg(MID_GRAY),
    tabs_selected: Style::new()
        .fg(BLACK)
        .bg(CYAN)
        .add_modifier(Modifier::BOLD),
    borders: Style::new().fg(MID_GRAY),
    description: Style::new().fg(LIGHT_GRAY),
    description_title: Style::new()
        .fg(WHITE)
        .add_modifier(Modifier::BOLD),
    key_binding: KeyBinding {
        key: Style::new().fg(BLACK).bg(LIGHT_GRAY),
        description: Style::new().fg(LIGHT_GRAY).bg(BLACK),
    },
    table: Table {
        header: Style::new().fg(YELLOW).add_modifier(Modifier::BOLD),
        row: Style::new().fg(LIGHT_GRAY),
        row_alt: Style::new().fg(LIGHT_GRAY).bg(DARK_BG),
        selected: Style::new()
            .fg(BLACK)
            .bg(LIGHT_YELLOW)
            .add_modifier(Modifier::BOLD),
    },
    sparkline: Sparkline {
        data: Style::new().fg(CYAN),
    },
};

pub const ERROR: Style = Style::new().fg(BRIGHT_RED).add_modifier(Modifier::BOLD);
pub const WARN: Style = Style::new().fg(BRIGHT_YELLOW);
pub const OK: Style = Style::new().fg(BRIGHT_GREEN);
pub const INFO: Style = Style::new().fg(BRIGHT_CYAN);
pub const MUTED: Style = Style::new().fg(MID_GRAY);

pub const DARK_BG: Color = Color::Rgb(30, 30, 40);
pub const CYAN: Color = Color::Rgb(64, 224, 224);
pub const YELLOW: Color = Color::Rgb(224, 192, 64);
pub const LIGHT_YELLOW: Color = Color::Rgb(255, 224, 128);
pub const BRIGHT_RED: Color = Color::Rgb(255, 80, 80);
pub const BRIGHT_YELLOW: Color = Color::Rgb(255, 220, 80);
pub const BRIGHT_GREEN: Color = Color::Rgb(80, 255, 120);
pub const BRIGHT_CYAN: Color = Color::Rgb(64, 240, 240);
pub const BRIGHT_MAGENTA: Color = Color::Rgb(255, 80, 255);
pub const BLACK: Color = Color::Rgb(0, 0, 0);
pub const MID_GRAY: Color = Color::Rgb(128, 128, 128);
pub const LIGHT_GRAY: Color = Color::Rgb(200, 200, 200);
pub const WHITE: Color = Color::Rgb(240, 240, 240);
