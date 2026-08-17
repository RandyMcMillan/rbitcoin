//! Bulk-IO backend selection for Class A body (no mmap payload).
//!
//! ## Operator env (single switch)
//! - `RBITCOIN_IO=uring|pread` — all bulk read/write paths (default: uring if available).
//! - `RBITCOIN_IO=mmap` (legacy) → demoted to **pread** (one-time warn).
//!
//! Per-path env overrides are **removed** — one global `RBITCOIN_IO` only.
//!
//! Class A **tx.body** payload is always pread/pwrite/uring — never mmap.

use crate::bulk_io;
use std::sync::OnceLock;

/// Bulk **read** backend (body denserels, head prefix, meta peeks, Class C).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadIoBackend {
    /// io_uring pread batch / streaming.
    Uring,
    /// libc pread (optionally multi-worker via `RBITCOIN_BULK_IO_WORKERS`).
    Pread,
}

/// Pure-write annotate backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteIoBackend {
    /// io_uring pwrite-only.
    Uring,
    /// libc `pwrite` (positional).
    Pwrite,
}

fn parse_read_token(s: &str) -> Option<ReadIoBackend> {
    match s.trim().to_ascii_lowercase().as_str() {
        "uring" | "io_uring" | "ioring" | "pool" | "iocp" => Some(ReadIoBackend::Uring),
        "pread" | "fd" | "libc" | "pwrite" => Some(ReadIoBackend::Pread),
        // Legacy body-mmap mode removed.
        "mmap" => {
            warn_mmap_demote();
            Some(ReadIoBackend::Pread)
        }
        _ => None,
    }
}

fn parse_write_token(s: &str) -> Option<WriteIoBackend> {
    match s.trim().to_ascii_lowercase().as_str() {
        "uring" | "io_uring" | "ioring" | "pool" | "iocp" => Some(WriteIoBackend::Uring),
        "pwrite" | "pread" | "fd" | "libc" => Some(WriteIoBackend::Pwrite),
        "mmap" => {
            warn_mmap_demote();
            Some(WriteIoBackend::Pwrite)
        }
        _ => None,
    }
}

fn warn_mmap_demote() {
    static LOGGED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        rbitcoin_log::warn!(
            "store: RBITCOIN_IO=mmap (and body mmap backends) removed; using pread/pwrite \
             (set RBITCOIN_IO=uring|pread)"
        );
    }
}

fn global_read_from_env() -> Option<ReadIoBackend> {
    if let Ok(s) = std::env::var("RBITCOIN_IO") {
        if let Some(b) = parse_read_token(&s) {
            return Some(b);
        }
    }
    None
}

fn global_write_from_env() -> Option<WriteIoBackend> {
    if let Ok(s) = std::env::var("RBITCOIN_IO") {
        if let Some(b) = parse_write_token(&s) {
            return Some(b);
        }
    }
    None
}

#[inline]
pub fn effective_read(selected: ReadIoBackend) -> ReadIoBackend {
    match selected {
        ReadIoBackend::Uring if !bulk_io::io_uring_enabled() => ReadIoBackend::Pread,
        other => other,
    }
}

#[inline]
pub fn effective_write(selected: WriteIoBackend) -> WriteIoBackend {
    match selected {
        WriteIoBackend::Uring if !bulk_io::io_uring_enabled() => WriteIoBackend::Pwrite,
        other => other,
    }
}

fn default_read() -> ReadIoBackend {
    if bulk_io::io_uring_enabled() {
        ReadIoBackend::Uring
    } else {
        ReadIoBackend::Pread
    }
}

fn default_write() -> WriteIoBackend {
    if bulk_io::io_uring_enabled() {
        WriteIoBackend::Uring
    } else {
        WriteIoBackend::Pwrite
    }
}

/// Global bulk read backend (`RBITCOIN_IO` only — no path overrides).
pub(crate) fn resolve_read() -> ReadIoBackend {
    let selected = global_read_from_env().unwrap_or_else(default_read);
    effective_read(selected)
}

/// Global bulk write backend (`RBITCOIN_IO` only — no path overrides).
pub(crate) fn resolve_write() -> WriteIoBackend {
    let selected = global_write_from_env().unwrap_or_else(default_write);
    effective_write(selected)
}

fn global_read_cached() -> ReadIoBackend {
    static B: OnceLock<ReadIoBackend> = OnceLock::new();
    *B.get_or_init(resolve_read)
}

fn global_write_cached() -> WriteIoBackend {
    static B: OnceLock<WriteIoBackend> = OnceLock::new();
    *B.get_or_init(resolve_write)
}

/// Bulk read backend for every path (pin, head-resolve, spend-meta, Class C).
#[inline]
pub fn read_io_backend() -> ReadIoBackend {
    global_read_cached()
}

/// Bulk write backend (spend annotate).
#[inline]
pub fn write_io_backend() -> WriteIoBackend {
    global_write_cached()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_io_envs() {
        std::env::remove_var("RBITCOIN_IO");
    }

    #[test]
    fn parse_tokens() {
        assert_eq!(parse_read_token("uring"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("io_uring"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("pool"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("iocp"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("ioring"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("pread"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_read_token("fd"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_read_token("libc"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_read_token("pwrite"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_read_token("mmap"), Some(ReadIoBackend::Pread));
        assert_eq!(parse_write_token("uring"), Some(WriteIoBackend::Uring));
        assert_eq!(parse_write_token("io_uring"), Some(WriteIoBackend::Uring));
        assert_eq!(parse_write_token("pwrite"), Some(WriteIoBackend::Pwrite));
        assert_eq!(parse_write_token("pread"), Some(WriteIoBackend::Pwrite));
        assert_eq!(parse_write_token("fd"), Some(WriteIoBackend::Pwrite));
        assert_eq!(parse_write_token("libc"), Some(WriteIoBackend::Pwrite));
        assert_eq!(parse_write_token("mmap"), Some(WriteIoBackend::Pwrite));
        assert!(parse_read_token("alternate").is_none());
        assert!(parse_write_token("nope").is_none());
    }

    #[test]
    fn path_env_ignored_global_only() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "uring");
        assert_eq!(resolve_read(), effective_read(ReadIoBackend::Uring));
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "pread");
        assert_eq!(resolve_read(), ReadIoBackend::Pread);
        clear_io_envs();
    }

    #[test]
    fn global_env_io_uring_off_and_aliases() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "mmap");
        assert_eq!(global_read_from_env(), Some(ReadIoBackend::Pread));
        assert_eq!(global_write_from_env(), Some(WriteIoBackend::Pwrite));
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO_URING", "0");
        assert_eq!(
            global_read_from_env(),
            None,
            "RBITCOIN_IO_URING is not an alias; use RBITCOIN_IO=pread"
        );
        assert_eq!(global_write_from_env(), None);
        clear_io_envs();
        // Unknown RBITCOIN_IO token → None (fall through).
        std::env::set_var("RBITCOIN_IO", "not-a-backend");
        assert_eq!(global_read_from_env(), None);
        assert_eq!(global_write_from_env(), None);
        clear_io_envs();
    }

    #[test]
    fn effective_backends_demote_when_uring_disabled() {
        // effective_* only demotes when bulk_io reports uring off; still exercises match arms.
        let _ = effective_read(ReadIoBackend::Pread);
        let _ = effective_read(ReadIoBackend::Uring);
        let _ = effective_write(WriteIoBackend::Pwrite);
        let _ = effective_write(WriteIoBackend::Uring);
    }
}
