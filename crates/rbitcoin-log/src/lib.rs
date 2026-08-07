//! Minimal process logger: leveled lines with UTC timestamps to stderr.
//!
//! No external crates. Configure with [`init`], [`init_from_env`], or
//! `RBITCOIN_LOG` / `--log-level` (wired by the node CLI).
//!
//! Format: `2026-07-15T19:21:03.456Z  INFO message…`

use std::fmt;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Log severity. Higher numeric values are more verbose.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Level {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        }
    }

    /// Parse `error|warn|info|debug|trace` (case-insensitive). Also accepts
    /// `warning` and single-letter forms `e|w|i|d|t`.
    pub fn parse(s: &str) -> Option<Level> {
        match s.trim().to_ascii_lowercase().as_str() {
            "error" | "e" => Some(Level::Error),
            "warn" | "warning" | "w" => Some(Level::Warn),
            "info" | "i" => Some(Level::Info),
            "debug" | "d" => Some(Level::Debug),
            "trace" | "t" => Some(Level::Trace),
            // Off: only never emit (set max below Error).
            "off" | "none" | "0" => None,
            _ => None,
        }
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Global max enabled level. 0 = off; default [`Level::Info`].
static MAX_LEVEL: AtomicU8 = AtomicU8::new(Level::Info as u8);

/// Set the maximum log level (inclusive).
pub fn init(level: Level) {
    MAX_LEVEL.store(level as u8, Ordering::Relaxed);
}

/// Disable all logging.
pub fn init_off() {
    MAX_LEVEL.store(0, Ordering::Relaxed);
}

/// Current max level, or `None` if logging is off.
pub fn max_level() -> Option<Level> {
    match MAX_LEVEL.load(Ordering::Relaxed) {
        0 => None,
        1 => Some(Level::Error),
        2 => Some(Level::Warn),
        3 => Some(Level::Info),
        4 => Some(Level::Debug),
        _ => Some(Level::Trace),
    }
}

/// Whether `level` would be emitted under the current max.
pub fn enabled(level: Level) -> bool {
    (level as u8) <= MAX_LEVEL.load(Ordering::Relaxed)
}

/// Read `RBITCOIN_LOG` (or optional `RUST_LOG` fallback for the first token).
///
/// Accepts a bare level (`info`) or `rbitcoin=debug` / `*=warn` style; the last
/// recognizable level token wins. Missing/invalid → leave current max unchanged
/// and return `false`.
pub fn init_from_env() -> bool {
    let raw = std::env::var("RBITCOIN_LOG")
        .or_else(|_| std::env::var("RUST_LOG"))
        .ok();
    let Some(raw) = raw else {
        return false;
    };
    if let Some(level) = parse_level_spec(&raw) {
        init(level);
        return true;
    }
    // Explicit off.
    if raw.trim().eq_ignore_ascii_case("off")
        || raw.trim().eq_ignore_ascii_case("none")
        || raw.trim() == "0"
    {
        init_off();
        return true;
    }
    false
}

/// Extract a [`Level`] from env-style specs (`debug`, `info,rbitcoin=trace`, …).
pub fn parse_level_spec(spec: &str) -> Option<Level> {
    let mut found = None;
    for part in spec.split([',', ';']) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // `crate=level` or bare `level`
        let token = part.rsplit('=').next().unwrap_or(part).trim();
        if let Some(l) = Level::parse(token) {
            found = Some(l);
        }
    }
    found
}

/// Format UTC timestamp `YYYY-MM-DDTHH:MM:SS.mmmZ`.
pub fn format_timestamp(now: SystemTime) -> String {
    let dur = now.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = dur.as_secs();
    let millis = dur.subsec_millis();
    let (y, mo, d, h, mi, s) = civil_utc(secs);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{millis:03}Z")
}

/// Civil UTC date/time from Unix seconds (proleptic Gregorian, no leap seconds).
fn civil_utc(secs: u64) -> (u32, u32, u32, u32, u32, u32) {
    let s = (secs % 60) as u32;
    let mins = secs / 60;
    let mi = (mins % 60) as u32;
    let hours = mins / 60;
    let h = (hours % 24) as u32;
    let days = hours / 24;

    // Days since 1970-01-01 → year/month/day (Howard Hinnant algorithm).
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    let y = (y + if m <= 2 { 1 } else { 0 }) as u32;
    (y, m, d, h, mi, s)
}

/// Optional line emphasis (ANSI on a TTY stderr only).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Style {
    #[default]
    Plain,
    /// Bold message body (`\x1b[1m…\x1b[0m`) when stderr is a terminal.
    Bold,
}

/// Write one log line if `level` is enabled.
pub fn log_at(level: Level, args: fmt::Arguments<'_>) {
    log_at_style(level, Style::Plain, args);
}

/// Write one log line with optional style. Bold is applied only when stderr is
/// an interactive terminal so redirected logs stay clean ASCII.
pub fn log_at_style(level: Level, style: Style, args: fmt::Arguments<'_>) {
    if !enabled(level) {
        return;
    }
    let ts = format_timestamp(SystemTime::now());
    let mut stderr = io::stderr().lock();
    // Bold only on interactive stderr so `> file` / pipes stay plain text.
    let bold = matches!(style, Style::Bold) && io::IsTerminal::is_terminal(&stderr);
    if bold {
        let _ = writeln!(stderr, "{ts} {level:<5} \x1b[1m{args}\x1b[0m");
    } else {
        let _ = writeln!(stderr, "{ts} {level:<5} {args}");
    }
    let _ = stderr.flush();
}

/// Crate identity for diagnostics and coverage scenarios.
pub fn crate_name() -> &'static str {
    "rbitcoin-log"
}

/// Log an error-level line.
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::log_at($crate::Level::Error, format_args!($($arg)*))
    };
}

/// Log a warning-level line.
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::log_at($crate::Level::Warn, format_args!($($arg)*))
    };
}

/// Log an info-level line.
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::log_at($crate::Level::Info, format_args!($($arg)*))
    };
}

/// Log an info-level line with bold emphasis (TTY only; plain when redirected).
#[macro_export]
macro_rules! info_bold {
    ($($arg:tt)*) => {
        $crate::log_at_style(
            $crate::Level::Info,
            $crate::Style::Bold,
            format_args!($($arg)*),
        )
    };
}

/// Log a debug-level line.
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        $crate::log_at($crate::Level::Debug, format_args!($($arg)*))
    };
}

/// Log a trace-level line.
#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        $crate::log_at($crate::Level::Trace, format_args!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_parse_and_order() {
        assert_eq!(Level::parse("INFO"), Some(Level::Info));
        assert_eq!(Level::parse("warning"), Some(Level::Warn));
        assert_eq!(Level::parse("t"), Some(Level::Trace));
        assert!(Level::Error < Level::Warn);
        assert!(Level::Warn < Level::Info);
        assert!(Level::Info < Level::Debug);
        assert_eq!(Level::Info.as_str(), "INFO");
        assert_eq!(format!("{}", Level::Warn), "WARN");
    }

    #[test]
    fn level_spec_env_style() {
        assert_eq!(parse_level_spec("debug"), Some(Level::Debug));
        assert_eq!(parse_level_spec("rbitcoin=trace"), Some(Level::Trace));
        assert_eq!(parse_level_spec("warn,net=debug"), Some(Level::Debug));
        assert_eq!(parse_level_spec("   "), None);
        assert_eq!(parse_level_spec("garbage"), None);
    }

    #[test]
    fn init_and_enabled() {
        init(Level::Warn);
        assert!(enabled(Level::Error));
        assert!(enabled(Level::Warn));
        assert!(!enabled(Level::Info));
        assert_eq!(max_level(), Some(Level::Warn));
        init_off();
        assert!(!enabled(Level::Error));
        assert_eq!(max_level(), None);
        init(Level::Info); // restore default for other tests in process
    }

    #[test]
    fn timestamp_epoch() {
        let s = format_timestamp(UNIX_EPOCH);
        assert_eq!(s, "1970-01-01T00:00:00.000Z");
        // 2024-01-01 00:00:00 UTC
        let s = format_timestamp(UNIX_EPOCH + std::time::Duration::from_secs(1_704_067_200));
        assert_eq!(s, "2024-01-01T00:00:00.000Z");
    }

    #[test]
    fn log_at_respects_level() {
        init(Level::Error);
        // Should not panic; visual check not required.
        log_at(Level::Info, format_args!("hidden"));
        log_at(Level::Error, format_args!("shown {}", 1));
        log_at_style(Level::Error, Style::Bold, format_args!("bold shown"));
        init(Level::Info);
    }

    #[test]
    fn style_default_is_plain() {
        assert_eq!(Style::default(), Style::Plain);
    }

    #[test]
    fn crate_name_stable() {
        assert_eq!(crate_name(), "rbitcoin-log");
    }

    #[test]
    fn level_as_str_all_variants() {
        assert_eq!(Level::Error.as_str(), "ERROR");
        assert_eq!(Level::Debug.as_str(), "DEBUG");
        assert_eq!(Level::Trace.as_str(), "TRACE");
        assert_eq!(Level::parse("off"), None);
        assert_eq!(Level::parse("none"), None);
        assert_eq!(Level::parse("0"), None);
        assert_eq!(Level::parse("e"), Some(Level::Error));
        assert_eq!(Level::parse("w"), Some(Level::Warn));
        assert_eq!(Level::parse("i"), Some(Level::Info));
        assert_eq!(Level::parse("d"), Some(Level::Debug));
    }

    #[test]
    fn max_level_maps_all_stored_values() {
        init(Level::Error);
        assert_eq!(max_level(), Some(Level::Error));
        init(Level::Debug);
        assert_eq!(max_level(), Some(Level::Debug));
        init(Level::Trace);
        assert_eq!(max_level(), Some(Level::Trace));
        assert!(enabled(Level::Trace));
        init(Level::Info);
    }

    #[test]
    fn init_from_env_off_and_level() {
        // Save/restore around env mutation for process safety.
        let prev_rb = std::env::var_os("RBITCOIN_LOG");
        let prev_rust = std::env::var_os("RUST_LOG");
        std::env::remove_var("RUST_LOG");
        std::env::set_var("RBITCOIN_LOG", "debug");
        assert!(init_from_env());
        assert_eq!(max_level(), Some(Level::Debug));
        std::env::set_var("RBITCOIN_LOG", "off");
        assert!(init_from_env());
        assert_eq!(max_level(), None);
        std::env::set_var("RBITCOIN_LOG", "none");
        assert!(init_from_env());
        std::env::set_var("RBITCOIN_LOG", "0");
        assert!(init_from_env());
        std::env::set_var("RBITCOIN_LOG", "not-a-level");
        assert!(!init_from_env());
        std::env::remove_var("RBITCOIN_LOG");
        // RUST_LOG fallback.
        std::env::set_var("RUST_LOG", "warn");
        assert!(init_from_env());
        assert_eq!(max_level(), Some(Level::Warn));
        std::env::remove_var("RBITCOIN_LOG");
        std::env::remove_var("RUST_LOG");
        assert!(!init_from_env());
        match prev_rb {
            Some(v) => std::env::set_var("RBITCOIN_LOG", v),
            None => std::env::remove_var("RBITCOIN_LOG"),
        }
        match prev_rust {
            Some(v) => std::env::set_var("RUST_LOG", v),
            None => std::env::remove_var("RUST_LOG"),
        }
        // max_level maps Warn branch explicitly.
        init(Level::Warn);
        assert_eq!(max_level(), Some(Level::Warn));
        // Bold style path (non-TTY → plain write arm still executed).
        init(Level::Error);
        log_at_style(Level::Error, Style::Bold, format_args!("bold-err"));
        init(Level::Info);
    }
}
