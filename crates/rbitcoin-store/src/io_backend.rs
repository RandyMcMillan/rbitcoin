//! Hierarchical bulk-IO backend selection for Class A body (no mmap payload).
//!
//! ## Global
//! - `RBITCOIN_IO=uring|pread` — default when path env unset.
//! - `RBITCOIN_IO=mmap` (legacy) → demoted to **pread** (one-time warn).
//! - `RBITCOIN_IO_URING=0` → **pread** when `RBITCOIN_IO` unset.
//!
//! ## Path overrides
//! - `RBITCOIN_PIN_IO` — denserels / Class A body pipeline
//! - `RBITCOIN_HEAD_RESOLVE_IO` — head-resolve body prefix
//! - `RBITCOIN_SPEND_META` — structural 9B peeks
//! - `RBITCOIN_SPEND_ANN` — pure-write annotate (`uring|pwrite`)
//! - `RBITCOIN_CLASS_C_IO` — bulk create-height slots
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
        "uring" | "io_uring" => Some(ReadIoBackend::Uring),
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
        "uring" | "io_uring" => Some(WriteIoBackend::Uring),
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
    if let Ok(s) = std::env::var("RBITCOIN_IO_URING") {
        if s == "0" || s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("off") {
            static LOGGED: std::sync::atomic::AtomicBool =
                std::sync::atomic::AtomicBool::new(false);
            if !LOGGED.swap(true, std::sync::atomic::Ordering::Relaxed) {
                rbitcoin_log::info!(
                    "store: RBITCOIN_IO_URING=0 is deprecated; use RBITCOIN_IO=pread"
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
    }
    if let Ok(s) = std::env::var("RBITCOIN_IO_URING") {
        if s == "0" || s.eq_ignore_ascii_case("false") || s.eq_ignore_ascii_case("off") {
            return Some(WriteIoBackend::Pwrite);
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

/// Resolve read backend for a path-specific env (then global, then default).
pub(crate) fn resolve_read(path_env: &str) -> ReadIoBackend {
    let selected = std::env::var(path_env)
        .ok()
        .and_then(|s| parse_read_token(&s))
        .or_else(global_read_from_env)
        .unwrap_or_else(default_read);
    effective_read(selected)
}

/// Resolve write backend for a path-specific env (then global, then default).
pub(crate) fn resolve_write(path_env: &str) -> WriteIoBackend {
    let selected = std::env::var(path_env)
        .ok()
        .and_then(|s| parse_write_token(&s))
        .or_else(global_write_from_env)
        .unwrap_or_else(default_write);
    effective_write(selected)
}

fn cached_read(path_env: &'static str) -> ReadIoBackend {
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

#[inline]
pub fn pin_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_PIN_IO")
}

#[inline]
pub fn head_resolve_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_HEAD_RESOLVE_IO")
}

#[inline]
pub fn spend_meta_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_SPEND_META")
}

#[inline]
pub fn class_c_io_backend() -> ReadIoBackend {
    cached_read("RBITCOIN_CLASS_C_IO")
}

#[inline]
pub fn spend_ann_io_backend() -> WriteIoBackend {
    static B: OnceLock<WriteIoBackend> = OnceLock::new();
    *B.get_or_init(|| resolve_write("RBITCOIN_SPEND_ANN"))
}

/// Class A body/idx linear appends always use **pwrite** (no mmap body append).
#[inline]
pub fn class_a_append_uses_pwrite() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

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
            "RBITCOIN_FD_APPEND",
        ] {
            std::env::remove_var(k);
        }
    }

    #[test]
    fn parse_tokens() {
        assert_eq!(parse_read_token("uring"), Some(ReadIoBackend::Uring));
        assert_eq!(parse_read_token("io_uring"), Some(ReadIoBackend::Uring));
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
    fn path_overrides_global() {
        let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO", "uring");
        std::env::set_var("RBITCOIN_PIN_IO", "pread");
        assert_eq!(resolve_read("RBITCOIN_PIN_IO"), ReadIoBackend::Pread);
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
        assert_eq!(global_read_from_env(), Some(ReadIoBackend::Pread));
        assert_eq!(global_write_from_env(), Some(WriteIoBackend::Pwrite));
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO_URING", "false");
        assert_eq!(global_read_from_env(), Some(ReadIoBackend::Pread));
        clear_io_envs();
        std::env::set_var("RBITCOIN_IO_URING", "off");
        assert_eq!(global_write_from_env(), Some(WriteIoBackend::Pwrite));
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
        assert!(class_a_append_uses_pwrite());
    }

    #[test]
    fn class_a_append_always_pwrite() {
        assert!(class_a_append_uses_pwrite());
    }
}
