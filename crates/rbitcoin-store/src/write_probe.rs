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

/// The dirty ranges of `path` a sync starting now covers. A write that
/// lands while the sync waits is not among them.
pub(crate) fn sync_begin(path: &Path) -> usize {
    dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(path)
        .map_or(0, Vec::len)
}

/// A sync made the first `n` dirty ranges of `path` durable, and the last
/// trailer written before it.
pub(crate) fn synced(path: &Path, n: usize) {
    if let Some(v) = dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_mut(path)
    {
        v.drain(..n.min(v.len()));
    }
    let mut trailers = trailers().lock().unwrap_or_else(|e| e.into_inner());
    if let Some(end) = trailers.0.get(path).copied() {
        trailers.1.insert(path.to_path_buf(), end);
    }
}

/// Bytes a write bypassing the `TableFile` door (bulk io_uring writes)
/// put past a file's end. They count as unsynced until a sync.
pub(crate) fn note_append(path: &Path, offset: u64, len: u64) {
    if len > 0 {
        note_dirty(path, offset, offset + len);
    }
}

/// (last trailer end written, last trailer end a sync covered) per file.
fn trailers() -> &'static Mutex<(HashMap<PathBuf, u64>, HashMap<PathBuf, u64>)> {
    static T: OnceLock<Mutex<(HashMap<PathBuf, u64>, HashMap<PathBuf, u64>)>> = OnceLock::new();
    T.get_or_init(|| Mutex::new((HashMap::new(), HashMap::new())))
}

/// Class A stems can live in a cold directory with no `meta` beside them.
const CLASS_A_STEMS: [&str; 16] = [
    "txout.body",
    "seqsigwit.body",
    "spent.body",
    "create.loc",
    "create.loc.ovf",
    "create.off",
    "seqsigwit.loc",
    "seqsigwit.loc.ovf",
    "seqsigwit.off",
    "txid.body",
    "txstat.body",
    "txstat.ovf",
    "txstat.blk",
    "input.loc",
    "input.off",
    "input.body",
];

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
    // A trailer moves with the length, so its durable end is the last one a
    // sync covered.
    let committed = if trailing {
        let t = trailers().lock().unwrap_or_else(|e| e.into_inner());
        t.1.get(path).copied().unwrap_or(header_len)
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
        if trailing {
            trailers()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .0
                .insert(path.to_path_buf(), end);
        }
        // The bytes the new length names must be synced, or imaged by a live
        // mark, before the length is safe.
        let named_safe = !unsynced_in(path, committed, new)
            || crate::page_guard::probe_armed_covers(
                path,
                committed,
                new.saturating_sub(committed),
            );
        if new <= committed {
            None
        } else if named_safe
            && crate::page_guard::probe_armed_covers(path, offset, len)
            && unsynced_in(path, committed, new)
        {
            Some("LEN_ARMED")
        } else if !named_safe && !trailing && new <= SECTOR {
            // The length field and every byte it names share sector 0, which
            // lands whole.
            Some("LEN_ONE_SECTOR")
        } else if !named_safe {
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
        let stem = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| CLASS_A_STEMS.contains(&n));
        let class = if stem || in_store(path) {
            class
        } else {
            "FIXTURE"
        };
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
    // The classifier passes these classes without reading frames, so they
    // skip the backtrace, which is most of the probe's cost.
    if SAFE_CLASSES.contains(&class) {
        write_entry(
            &out,
            format!("{class} {parent}/{name}"),
            committed,
            offset,
            len,
            &[],
        );
        return;
    }
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
    // Every distinct stack is its own entry: a test hook reaching a
    // production write first must not hide the production stack after it.
    let stack = {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        frames.hash(&mut h);
        h.finish()
    };
    let key = format!("{class} {parent}/{name} {caller}#{stack:016x}");
    let rest: Vec<String> = frames.into_iter().skip(1).collect();
    write_entry(&out, key, committed, offset, len, &rest);
}

/// Classes `scripts/write-probe-classify.py` passes as safe by structure.
/// Keep the two lists the same.
const SAFE_CLASSES: &[&str] = &[
    "CREATE",
    "ARMED",
    "ONE_SECTOR",
    "LEN_ONE_SECTOR",
    "LEN_ARMED",
    "LEN_AFTER_SYNC",
    "FIXTURE",
];

/// Append one entry the first time its key is seen.
fn write_entry(
    out: &std::ffi::OsStr,
    key: String,
    committed: u64,
    offset: u64,
    len: u64,
    frames: &[String],
) {
    use std::io::Write as _;
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
    for f in frames {
        line.push_str("    ");
        line.push_str(f);
        line.push('\n');
    }
    if let Ok(mut fh) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
    {
        let _ = fh.write_all(line.as_bytes());
    }
}
