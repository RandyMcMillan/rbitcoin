//! Optional JSONL API call log (`--api-log PATH`).
//!
//! Always emits a DEBUG `api:` line. When [`init_api_log`] has been called,
//! also appends one JSON object per call so the operator can `tail -f` without
//! turning the main log up.

use crate::{debug, format_timestamp};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;
use std::time::SystemTime;

const PARAMS_MAX: usize = 384;

static API_LOG: Mutex<Option<File>> = Mutex::new(None);

/// Open (append) `path` as the process API call log.
pub fn init_api_log(path: impl AsRef<Path>) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let f = OpenOptions::new().create(true).append(true).open(path)?;
    let mut g = API_LOG.lock().unwrap_or_else(|e| e.into_inner());
    *g = Some(f);
    Ok(())
}

/// Stop writing the API call file (tests).
pub fn close_api_log() {
    let mut g = API_LOG.lock().unwrap_or_else(|e| e.into_inner());
    *g = None;
}

/// True after a successful [`init_api_log`] (tests / diagnostics).
pub fn api_log_enabled() -> bool {
    API_LOG.lock().unwrap_or_else(|e| e.into_inner()).is_some()
}

/// Truncate a params blob for the log (UTF-8 safe).
pub fn compact_params(params: &str) -> String {
    if params.len() <= PARAMS_MAX {
        return params.to_string();
    }
    let mut end = PARAMS_MAX;
    while end > 0 && !params.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &params[..end])
}

/// Record one Electrum / Esplora / RPC call.
///
/// `params` should already be compact (see [`compact_params`]). `err` is
/// `None` on success.
pub fn api_call(
    surface: &str,
    peer: &str,
    method: &str,
    params: &str,
    wall_ms: u64,
    err: Option<&str>,
) {
    let params = compact_params(params);
    match err {
        None => debug!("api: {surface} peer={peer} {method} {params} wall_ms={wall_ms} ok"),
        Some(e) => debug!("api: {surface} peer={peer} {method} {params} wall_ms={wall_ms} err={e}"),
    }

    let mut g = API_LOG.lock().unwrap_or_else(|e| e.into_inner());
    let Some(file) = g.as_mut() else {
        return;
    };
    let ts = format_timestamp(SystemTime::now());
    let ok = err.is_none();
    let err_json = match err {
        None => "null".to_string(),
        Some(e) => format!("\"{}\"", json_escape(e)),
    };
    let line = format!(
        "{{\"ts\":\"{ts}\",\"surface\":\"{}\",\"peer\":\"{}\",\"method\":\"{}\",\"params\":\"{}\",\"wall_ms\":{wall_ms},\"ok\":{ok},\"err\":{err_json}}}\n",
        json_escape(surface),
        json_escape(peer),
        json_escape(method),
        json_escape(&params),
    );
    let _ = file.write_all(line.as_bytes());
    let _ = file.flush();
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static TEST_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn compact_params_truncates() {
        assert_eq!(compact_params("[]"), "[]");
        let long = "x".repeat(500);
        let c = compact_params(&long);
        assert!(c.ends_with('…'));
        assert!(c.len() < 400);
    }

    #[test]
    fn api_log_file_records_json_line() {
        let _g = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let path = std::env::temp_dir().join(format!(
            "rbitcoin-api-log-{}-{}.jsonl",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let _ = std::fs::remove_file(&path);
        init_api_log(&path).unwrap();
        assert!(api_log_enabled());
        api_call(
            "electrum",
            "127.0.0.1:1",
            "blockchain.tweaks.subscribe",
            "[0,1,false]",
            12,
            None,
        );
        api_call(
            "electrum",
            "127.0.0.1:1",
            "no.such",
            "[]",
            1,
            Some("unknown method: no.such"),
        );
        close_api_log();
        let body = std::fs::read_to_string(&path).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(body.contains("\"method\":\"blockchain.tweaks.subscribe\""));
        assert!(body.contains("\"params\":\"[0,1,false]\""));
        assert!(body.contains("\"ok\":true"));
        assert!(body.contains("\"ok\":false"));
        assert!(body.contains("unknown method"));
    }

    #[test]
    fn json_escape_quotes() {
        assert_eq!(json_escape("a\"b\\c"), "a\\\"b\\\\c");
    }
}
