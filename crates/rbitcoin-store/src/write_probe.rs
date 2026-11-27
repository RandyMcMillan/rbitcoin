//! Write probe (`write-probe` feature, CI only). Classify every TableFile
//! write that lands below the durable end, and every length publish, by the
//! sector rule and the sync order. One line per distinct (class, file,
//! caller) goes to `$WRITE_PROBE_OUT`. `scripts/write-probe-classify.py`
//! fails CI on a class with no reviewed reason.

#![allow(clippy::type_complexity)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const SECTOR: u64 = 512;

fn dirty() -> &'static Mutex<HashMap<PathBuf, Vec<(u64, u64)>>> {
    static D: OnceLock<Mutex<HashMap<PathBuf, Vec<(u64, u64)>>>> = OnceLock::new();
    D.get_or_init(|| Mutex::new(HashMap::new()))
}

/// A sync made every written byte of `path` durable.
pub(crate) fn synced(path: &Path) {
    dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path);
}

fn unsynced_in(path: &Path, from: u64, to: u64) -> bool {
    dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(path)
        .is_some_and(|v| v.iter().any(|&(s, e)| s < to && e > from))
}

fn note_dirty(path: &Path, from: u64, to: u64) {
    dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(path.to_path_buf())
        .or_default()
        .push((from, to));
}

/// Called with the durable `committed` end before every TableFile pwrite.
pub(crate) fn write(
    path: &Path,
    committed: u64,
    offset: u64,
    bytes: &[u8],
    trailing: bool,
    header_len: u64,
) {
    let len = bytes.len() as u64;
    if len == 0 {
        return;
    }
    // The header in the file, not the RAM length: a publish moves RAM first.
    let committed = if trailing {
        committed
    } else {
        header_on_file(path).unwrap_or(committed)
    };
    let end = offset + len;
    let len_field = if trailing {
        len == 32 && end >= committed.saturating_sub(32)
    } else {
        offset == 8 && len == 8
    };
    let class = if len_field {
        let new = if trailing {
            end
        } else {
            u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]))
        };
        if new <= committed {
            None
        } else if committed <= header_len {
            Some("LEN_CREATE")
        } else if crate::page_guard::probe_armed_covers(path, offset, len) {
            Some("LEN_ARMED")
        } else if unsynced_in(path, committed, new) {
            Some("LEN_BEFORE_SYNC")
        } else {
            Some("LEN_AFTER_SYNC")
        }
    } else if offset >= committed {
        None
    } else if committed <= header_len {
        Some("CREATE")
    } else if crate::page_guard::probe_armed_covers(path, offset, len) {
        Some("ARMED")
    } else if offset / SECTOR == (end - 1) / SECTOR {
        Some("ONE_SECTOR")
    } else {
        Some("MULTI_SECTOR")
    };
    if !len_field {
        note_dirty(path, offset, end);
    }
    if let Some(class) = class {
        let class = if in_store(path) { class } else { "FIXTURE" };
        log(class, path, committed, offset, len);
    }
}

/// A store has `meta` beside its files, or beside their directory.
fn in_store(path: &Path) -> bool {
    let dir = path.parent();
    dir.is_some_and(|d| d.join("meta").is_file())
        || dir
            .and_then(Path::parent)
            .is_some_and(|d| d.join("meta").is_file())
}

fn header_on_file(path: &Path) -> Option<u64> {
    use std::os::unix::fs::FileExt as _;
    let f = std::fs::File::open(path).ok()?;
    let mut b = [0u8; 8];
    f.read_exact_at(&mut b, 8).ok()?;
    Some(u64::from_le_bytes(b))
}

fn log(class: &str, path: &Path, committed: u64, offset: u64, len: u64) {
    use std::io::Write as _;
    let Some(out) = std::env::var_os("WRITE_PROBE_OUT") else {
        return;
    };
    let strip = |s: &str| -> String { s.chars().filter(|c| !c.is_ascii_digit()).collect() };
    let name = strip(
        &path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default(),
    );
    let parent = path
        .parent()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let parent = if parent.contains('.') || parent == "guard" {
        strip(&parent)
    } else {
        "<tmp>".to_string()
    };
    let bt = std::backtrace::Backtrace::force_capture().to_string();
    let frames: Vec<String> = bt
        .lines()
        .map(str::trim)
        .filter(|l| l.contains("rbitcoin_"))
        .filter(|l| !l.contains("write_probe::") && !l.contains("::file::"))
        .map(|l| l.split_once(": ").map_or(l, |(_, f)| f).to_string())
        .take(8)
        .collect();
    // A write straight from a test body has no rbitcoin_ frame above it.
    let test_frame = bt
        .lines()
        .map(str::trim)
        .find(|l| l.contains("suite::") || l.contains("tests::"))
        .map(|l| l.split_once(": ").map_or(l, |(_, f)| f).to_string());
    let caller = frames
        .first()
        .cloned()
        .or_else(|| test_frame.map(|t| format!("test-body:{t}")))
        .unwrap_or_else(|| format!("?thread={}", std::thread::current().name().unwrap_or("?")));
    let key = format!("{class} {parent}/{name} {caller}");
    static SEEN: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    if !SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(key.clone())
    {
        return;
    }
    let mut line = format!("PROBE {key} end={committed} off={offset} len={len}\n");
    for f in frames.iter().skip(1) {
        line.push_str("    ");
        line.push_str(f);
        line.push('\n');
    }
    if let Ok(mut fh) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out)
    {
        let _ = fh.write_all(line.as_bytes());
    }
}
