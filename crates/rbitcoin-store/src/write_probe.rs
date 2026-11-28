//! Write probe (`write-probe` feature, CI only). Classify every TableFile
//! write that lands below the durable end, including a bulk body write that
//! bypasses the door, and every length publish, by the
//! sector rule and the sync order. One line per distinct (class, file,
//! caller) goes to `$WRITE_PROBE_OUT`. `scripts/write-probe-classify.py`
//! fails CI on a class with no reviewed reason.

#![allow(clippy::type_complexity)]

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

const SECTOR: u64 = 512;

/// What a door write carries. The caller names it: a payload can have any
/// length and offset, so the probe does not infer a publish from its shape.
#[derive(Clone, Copy, PartialEq, Eq)]
#[doc(hidden)]
pub enum WriteKind {
    Payload,
    /// A header length field or a trailing footer.
    Length,
}

/// Unsynced byte ranges per file. A landed range carries the per-file
/// sequence number it took when it landed. A pending range is still in
/// flight: no sync clears it.
#[derive(Default)]
struct Dirty {
    next: u64,
    ranges: Vec<(u64, u64, u64)>,
    next_pending: u64,
    pending: Vec<(u64, u64, u64)>,
}

fn dirty() -> &'static Mutex<HashMap<PathBuf, Dirty>> {
    static D: OnceLock<Mutex<HashMap<PathBuf, Dirty>>> = OnceLock::new();
    D.get_or_init(|| Mutex::new(HashMap::new()))
}

/// What a sync starting now covers: the writes made before it, and the last
/// trailer written before it.
#[doc(hidden)]
pub struct SyncPoint {
    seq: u64,
    trailer: Option<u64>,
}

/// A sync covers only the writes that landed before it began. A write that
/// lands while it waits, or is still in flight, is not covered by it.
#[doc(hidden)]
pub fn sync_begin(path: &Path) -> SyncPoint {
    let seq = dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(path)
        .map_or(0, |d| d.next);
    let trailer = trailers()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .0
        .get(path)
        .copied();
    SyncPoint { seq, trailer }
}

/// A sync that began at `at` finished: the writes and trailer it covered are
/// durable.
#[doc(hidden)]
pub fn synced(path: &Path, at: SyncPoint) {
    if let Some(d) = dirty()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get_mut(path)
    {
        d.ranges.retain(|&(seq, _, _)| seq >= at.seq);
    }
    if let Some(end) = at.trailer {
        trailers()
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .1
            .insert(path.to_path_buf(), end);
    }
}

/// A trailing table opened from disk: its footer end is durable. Only a
/// path the probe has no record of takes it, so a table this process
/// created and has not synced keeps no durable end.
#[doc(hidden)]
pub fn opened_trailing(path: &Path, end: u64) {
    let mut t = trailers().lock().unwrap_or_else(|e| e.into_inner());
    if t.0.contains_key(path) || t.1.contains_key(path) {
        return;
    }
    t.0.insert(path.to_path_buf(), end);
    t.1.insert(path.to_path_buf(), end);
}

/// A trailing table created at `path` has no durable end yet, whatever an
/// earlier file at that path had.
pub(crate) fn created_trailing(path: &Path) {
    let mut t = trailers().lock().unwrap_or_else(|e| e.into_inner());
    t.0.remove(path);
    t.1.remove(path);
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
        .is_some_and(|d| {
            d.ranges
                .iter()
                .chain(d.pending.iter())
                .any(|&(_, s, e)| s < to && e > from)
        })
}

/// A write [`write`] classified and its caller has not yet finished. Drop it
/// once the write returns, `Ok` or `Err`: a failed write may have landed
/// some bytes. A payload range takes its sequence number then, so a sync
/// that began while it was in flight does not clear it. A trailer becomes
/// the last one written only through [`Landing::landed`]: after an `Err`
/// the file's footer is unknown, so the previous end stays.
#[must_use = "hold it until the write returns"]
#[doc(hidden)]
pub struct Landing {
    path: PathBuf,
    payload: Option<u64>,
    trailer: Option<u64>,
    ok: bool,
}

impl Landing {
    /// The write returned `Ok`.
    pub fn landed(mut self) {
        self.ok = true;
    }
}

impl Drop for Landing {
    fn drop(&mut self) {
        if let Some(id) = self.payload {
            let mut map = dirty().lock().unwrap_or_else(|e| e.into_inner());
            if let Some(d) = map.get_mut(&self.path) {
                if let Some(i) = d.pending.iter().position(|&(p, _, _)| p == id) {
                    let (_, from, to) = d.pending.swap_remove(i);
                    d.ranges.push((d.next, from, to));
                    d.next += 1;
                }
            }
        }
        if let Some(end) = self.trailer.filter(|_| self.ok) {
            trailers()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .0
                .insert(self.path.clone(), end);
        }
    }
}

fn note_pending(path: &Path, from: u64, to: u64) -> u64 {
    let mut map = dirty().lock().unwrap_or_else(|e| e.into_inner());
    let d = map.entry(path.to_path_buf()).or_default();
    let id = d.next_pending;
    d.pending.push((id, from, to));
    d.next_pending += 1;
    id
}

/// Called with the durable `committed` end before every TableFile pwrite,
/// and before a bulk write that bypasses the door. Hold the [`Landing`]
/// until the write returns.
pub(crate) fn write(
    path: &Path,
    committed: u64,
    offset: u64,
    bytes: &[u8],
    trailing: bool,
    header_len: u64,
    kind: WriteKind,
) -> Landing {
    let (class, committed, landing) =
        classify(path, committed, offset, bytes, trailing, header_len, kind);
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
        log(class, path, committed, offset, bytes.len() as u64);
    }
    landing
}

/// The class of one write, the durable end it was judged against, and the
/// landing that records it.
#[doc(hidden)]
pub fn classify(
    path: &Path,
    committed: u64,
    offset: u64,
    bytes: &[u8],
    trailing: bool,
    header_len: u64,
    kind: WriteKind,
) -> (Option<&'static str>, u64, Landing) {
    let mut landing = Landing {
        path: path.to_path_buf(),
        payload: None,
        trailer: None,
        ok: false,
    };
    let len = bytes.len() as u64;
    if len == 0 {
        return (None, committed, landing);
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
    let len_field = kind == WriteKind::Length;
    let class = if len_field {
        // An 8-byte length field holds the new length. A trailing footer
        // ends at it.
        let new = match <[u8; 8]>::try_from(bytes) {
            Ok(field) => u64::from_le_bytes(field),
            Err(_) => end,
        };
        if trailing {
            landing.trailer = Some(new);
        }
        // A trailing length counts its footer, so the payload it names ends
        // one footer earlier, and starts where the old footer sat.
        let footer = if trailing {
            crate::file::TRAILING_FOOTER_LEN as u64
        } else {
            0
        };
        let (lo, hi) = (committed.saturating_sub(footer), new.saturating_sub(footer));
        // The bytes the new length names must be synced, or imaged by a live
        // mark, before the length is safe.
        let named_unsynced = unsynced_in(path, lo, hi);
        let named_safe = !named_unsynced
            || crate::page_guard::probe_armed_covers(path, lo, hi.saturating_sub(lo));
        if new <= committed {
            // A length that does not grow names only durable bytes. The
            // rewrite itself can still tear across sectors.
            Some(in_place_class(path, offset, len))
        } else if crate::page_guard::probe_armed_touches(path, lo, hi.saturating_sub(lo))
            && !crate::page_guard::probe_armed_covers(path, offset, len)
        {
            // A restore after a crash rewrites the marked pages under the
            // bytes this length names from the mark's images, and leaves the
            // length. Synced or not, those bytes are the mark's, not the
            // length's. The arm map is per file, so "the mark" is the file's
            // live arm.
            Some("LEN_ON_MARK")
        } else if named_safe
            && crate::page_guard::probe_armed_covers(path, offset, len)
            && named_unsynced
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
    } else {
        Some(in_place_class(path, offset, len))
    };
    if !len_field {
        landing.payload = Some(note_pending(path, offset, end));
    }
    (class, committed, landing)
}

/// A write over durable bytes: safe when a live mark images its pages or it
/// stays inside one sector.
fn in_place_class(path: &Path, offset: u64, len: u64) -> &'static str {
    if crate::page_guard::probe_armed_covers(path, offset, len) {
        "ARMED"
    } else if offset / SECTOR == (offset + len - 1) / SECTOR {
        "ONE_SECTOR"
    } else {
        "MULTI_SECTOR"
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

/// Append one entry the first time its key is seen. A log that cannot be
/// written fails the run: a dropped entry would let the classifier pass
/// writes it never saw.
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
    let mut seen = SEEN
        .get_or_init(|| Mutex::new(HashSet::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if seen.contains(&key) {
        return;
    }
    let mut line = format!("PROBE {key} end={committed} off={offset} len={len}\n");
    for f in frames {
        line.push_str("    ");
        line.push_str(f);
        line.push('\n');
    }
    let wrote = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(out)
        .and_then(|mut fh| fh.write_all(line.as_bytes()));
    if let Err(e) = wrote {
        panic!(
            "write probe: cannot append to {}: {e}",
            Path::new(out).display()
        );
    }
    seen.insert(key);
}
