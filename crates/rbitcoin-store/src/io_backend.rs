//! Hierarchical bulk-IO backend selection.
//!
//! ## Global
//! - `RBITCOIN_IO=mmap|uring|pread` — default for all bulk sites when path env unset.
//! - `RBITCOIN_IO_URING=0` — compat: same as global `pread` when `RBITCOIN_IO` unset.
//!
//! ## Path overrides (win over global)
//! - `RBITCOIN_PIN_IO` — denserels / Class A body pipeline
//! - `RBITCOIN_HEAD_RESOLVE_IO` — archive head-resolve body prefix
//! - `RBITCOIN_SPEND_META` — structural 9B meta peeks
//! - `RBITCOIN_SPEND_ANN` — pure-write annotate (`mmap|uring|pwrite`)
//! - `RBITCOIN_CLASS_C_IO` — bulk create-height slots
//!
//! **No alternate modes.** When `uring` is selected but the ring is unavailable,
//! demote to `pread` (reads) or `pwrite`/`mmap` (ann).

use crate::bulk_io;
use std::sync::OnceLock;

/// Bulk **read** backend (body denserels, head prefix, meta peeks, Class C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIoBackend {
    /// One map pin + memcpy / peeks (no fd per offset).
    Mmap,
    /// io_uring pread batch / streaming.
    Uring,
    /// libc pread (optionally multi-worker via `RBITCOIN_BULK_IO_WORKERS`).
    Pread,
}

/// Pure-write annotate backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteIoBackend {
    /// mmap `write_at` into body epoch.
    Mmap,
    /// io_uring pwrite-only.
    Uring,
    /// libc `pwrite` (positional).
    Pwrite,
}

fn parse_read_token(s: &str) -> Option<ReadIoBackend> {
    match s.trim().to_ascii_lowercase().as_str() {
        "mmap" => Some(ReadIoBackend::Mmap),
        "uring" | "io_uring" => Some(ReadIoBackend::Uring),
        "pread" | "fd" | "libc" => Some(ReadIoBackend::Pread),
        // Ann path word used by mistake → treat as pread (fd path).
        "pwrite" => Some(ReadIoBackend::Pread),
        _ => None,
    }
}

fn parse_write_token(s: &str) -> Option<WriteIoBackend> {
    match s.trim().to_ascii_lowercase().as_str() {
        "mmap" => Some(WriteIoBackend::Mmap),
        "uring" | "io_uring" => Some(WriteIoBackend::Uring),
        "pwrite" | "pread" | "fd" | "libc" => Some(WriteIoBackend::Pwrite),
        _ => None,
    }
}

/// Global `RBITCOIN_IO` or compat `RBITCOIN_IO_URING=0` → pread.
fn global_read_from_env() -> Option<ReadIoBackend> {
    if let Ok(s) = std::env::var("RBITCOIN_IO") {
        if let Some(b) = parse_read_token(&s) {
            return Some(b);
        }
    }
    // Compat: old kill-switch.
    if let Ok(s) = std::env::var("RBITCOIN_IO_URING") {
        if s == "0" || s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("off") {
            static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
            if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                rbitcoin_log::info!(
                    "store: RBITCOIN_IO_URING=0 is deprecated; use RBITCOIN_IO=pread \
                     (or per-path RBITCOIN_*_IO)"
                );
            }
            return Some(ReadIoBackend::Pread);
        }
    }
    None
}

fn global_write_from_env() -> Option<WriteIoBackend> {
    if let Ok(s) = std::env::var("RBITCOIN_IO") {
        if let Some(b) = parse_write_token(&s) {
            return Some(b);
        }
        // Map global pread → pwrite for annotate.
        if let Some(ReadIoBackend::Pread) = parse_read_token(&s) {
            return Some(WriteIoBackend::Pwrite);
        }
        if let Some(ReadIoBackend::Mmap) = parse_read_token(&s) {
            return Some(WriteIoBackend::Mmap);
        }
        if let Some(ReadIoBackend::Uring) = parse_read_token(&s) {
            return Some(WriteIoBackend::Uring);
        }
    }
    if let Ok(s) = std::env::var("RBITCOIN_IO_URING") {
        if s == "0" || s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("off") {
            return Some(WriteIoBackend::Pwrite);
        }
    }
    None
}

/// Demote uring → pread when ring unavailable.
#[inline]
pub fn effective_read(selected: ReadIoBackend) -> ReadIoBackend {
    match selected {
        ReadIoBackend::Uring if !bulk_io::io_uring_enabled() => ReadIoBackend::Pread,
        other => other,
    }
}

/// Demote uring → pwrite when ring unavailable (mmap remains if already mmap).
#[inline]
pub fn effective_write(selected: WriteIoBackend) -> WriteIoBackend {
    match selected {
        WriteIoBackend::Uring if !bulk_io::io_uring_enabled() => WriteIoBackend::Pwrite,
        other => other,
    }
}

/// Default when nothing set: uring if available else pread.
fn default_read() -> ReadIoBackend {
    if bulk_io::io_uring_enabled() {
        ReadIoBackend::Uring
    } else {
        ReadIoBackend::Pread
    }
}

/// Default ann: uring if available else mmap.
fn default_write() -> WriteIoBackend {
    if bulk_io::io_uring_enabled() {
        WriteIoBackend::Uring
    } else {
        WriteIoBackend::Mmap
    }
}

fn resolve_read(path_env: &str) -> ReadIoBackend {
    let selected = std::env::var(path_env)
        .ok()
        .and_then(|s| parse_read_token(&s))
        .or_else(global_read_from_env)
        .unwrap_or_else(default_read);
    effective_read(selected)
}

fn resolve_write(path_env: &str) -> WriteIoBackend {
    let selected = std::env::var(path_env)
        .ok()
        .and_then(|s| parse_write_token(&s))
        .or_else(global_write_from_env)
        .unwrap_or_else(default_write);
    effective_write(selected)
}

// ── Cached per-path selection (env is process-stable) ─────────────────────

fn cached_read(path_env: &'static str) -> ReadIoBackend {
    // OnceLock per path via match — simple statics.
    match path_env {
        "RBITCOIN_PIN_IO" => {
            static B: OnceLock<ReadIoBackend> = OnceLock::new();
            *B.get_or_init(|| resolve_read("RBITCOIN_PIN_IO"))
        }
        "RBITCOIN_HEAD_RESOLVE_IO" => {
            static B: OnceLock<ReadIoBackend> = OnceLock::new();
            *B.get_or_init(|| resolve_read("RBITCOIN_HEAD_RESOLVE_IO"))
        }
        "RBITCOIN_SPEND_META" => {
            static B: OnceLock<ReadIoBackend> = OnceLock::new();
            *B.get_or_init(|| resolve_read("RBITCOIN_SPEND_META"))
        }
        "RBITCOIN_CLASS_C_IO" => {
            static B: OnceLock<ReadIoBackend> = OnceLock::new();
            *B.get_or_init(|| resolve_read("RBITCOIN_CLASS_C_IO"))
        }
        other => resolve_read(other),
    }
}

/// Pin denserels / Class A body pipeline backend.
#[inline]
pub fn pin_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_PIN_IO")
}

/// Archive head-resolve body-prefix backend.
#[inline]
pub fn head_resolve_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_HEAD_RESOLVE_IO")
}

/// Structural spender-meta 9B bulk read backend.
#[inline]
pub fn spend_meta_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_SPEND_META")
}

/// Class C bulk create-height backend.
#[inline]
pub fn class_c_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_CLASS_C_IO")
}

/// Pure-write spend annotate backend.
#[inline]
pub fn spend_ann_io_backend() -> WriteIoBackend {
    static B: OnceLock<WriteIoBackend> = OnceLock::new();
    *B.get_or_init(|| resolve_write("RBITCOIN_SPEND_ANN"))
}

/// Force a read backend for tests (does not re-read env; use before first call
/// or call only from tests that set env before any resolve).
#[cfg(test)]
pub fn resolve_read_for_test(path_env: &str) -> ReadIoBackend {
    resolve_read(path_env)
}

#[cfg(test)]
pub fn resolve_write_for_test(path_env: &str) -> WriteIoBackend {
    resolve_write(path_env)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Env mutation is process-global; serialize tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_io_envs() {
        for k in [
            "RBITCOIN_IO",
            "RBITCOIN_IO_URING",
            "RBITCOIN_PIN_IO",
            "RBITCOIN_HEAD_RESOLVE_IO",
            "RBITCOIN_SPEND_META",
            "RBITCOIN_SPEND_ANN",
            "RBITCOIN_CLASS_C_IO",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn parse_tokens() {
        assert_eq!(parse_read_token("mmap"), Some(ReadIoBackend::Mmap));
        assert_eq!(parse_read_token("URING"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("pread"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_write_token("pwrite"), Some(WriteIoBackend::Pwrite));
        assert_eq!(parse_write_token("mmap"), Some(WriteIoBackend::Mmap));
        assert!(parse_read_token("alternate").is_none());
        assert!(parse_read_token("").is_none());
    }

    #[test]
    fn path_overrides_global() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "uring");
        std::env::set_var("RBITCOIN_PIN_IO", "mmap");
        // resolve_read does not use OnceLock of pin_io_backend (cached) — use for_test
        let b = resolve_read_for_test("RBITCOIN_PIN_IO");
        assert_eq!(effective_read(b), ReadIoBackend::Mmap);
        clear_io_envs();
    }

    #[test]
    fn global_io_used_when_path_unset() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "mmap");
        let b = resolve_read_for_test("RBITCOIN_PIN_IO");
        assert_eq!(b, ReadIoBackend::Mmap);
        clear_io_envs();
    }

    #[test]
    fn io_uring_zero_compat_is_pread() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO_URING", "0");
        let b = resolve_read_for_test("RBITCOIN_HEAD_RESOLVE_IO");
        assert_eq!(b, ReadIoBackend::Pread);
        clear_io_envs();
    }

    #[test]
    fn global_pread_maps_ann_to_pwrite() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "pread");
        let b = resolve_write_for_test("RBITCOIN_SPEND_ANN");
        assert_eq!(b, WriteIoBackend::Pwrite);
        clear_io_envs();
    }
}
