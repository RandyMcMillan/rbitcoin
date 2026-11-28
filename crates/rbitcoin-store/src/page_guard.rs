//! Full-page images published by rename.
//!
//! [`arm_page_guard_group`] writes every 4 KiB image the next live writes will
//! dirty, then syncs and renames the mark. Each record names its file,
//! because each file has its own page 0. [`restore_page_guard_group`] copies
//! those images onto the live files and removes the mark. It does not read the
//! live bytes.
//! [`append_past_end`] writes only bytes at or past `committed_end`, which may
//! share the committed tail block (crash-recovery.md, the sector rule).

use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use rbitcoin_primitives::{schema_file_openable, SCHEMA_VERSION, STORE_MAGIC};

use crate::durability::Durable;
use crate::error::StoreError;
use crate::file::{
    fsync_parent_dir, handle_pread_all, handle_pwrite_all, write_file_synced_tmp_rename, TableFile,
};

pub const PAGE: u64 = crate::tx_table::BODY_PAGE_SIZE;
const HEAD: usize = 12;
const RECORD: usize = 8 + PAGE as usize;
const GROUP_KIND: u16 = 1;

/// The one directory under a store that holds marks. Open looks only here.
pub(crate) const GUARD_DIR: &str = "guard";

/// Forward images for `confirmed[]`, `header_txs_*`, and `strong_tx`.
pub const CLASS_C_MARK: &str = "guard/class_c";
/// Empty flag: the first `header.body` append may not be in `header.head`.
pub const HEADER_BODY_MARK: &str = "guard/header.body";
/// Forward images for one `header.head` generation.
pub const HEADER_HEAD_MARK: &str = "guard/header.head";
/// Prefix of one scripthash-commit mark (`guard/sh-00`, `guard/sh-ovf`).
pub const SH_MARK_PREFIX: &str = "guard/sh-";

fn armed_map() -> &'static Mutex<HashMap<PathBuf, HashSet<u64>>> {
    static ARMED: OnceLock<Mutex<HashMap<PathBuf, HashSet<u64>>>> = OnceLock::new();
    ARMED.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Per `guard/` directory, the live files a mark change dropped whose
/// directory sync failed. The next sync of that directory makes the change
/// durable and releases them, even when the retry itself drops nothing.
fn pending_release_map() -> &'static Mutex<HashMap<PathBuf, HashSet<PathBuf>>> {
    static PENDING: OnceLock<Mutex<HashMap<PathBuf, HashSet<PathBuf>>>> = OnceLock::new();
    PENDING.get_or_init(|| Mutex::new(HashMap::new()))
}

fn guard_live_dir(mark: &Path) -> Option<&Path> {
    mark.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .and_then(|parent| parent.parent())
        .filter(|dir| !dir.as_os_str().is_empty())
}

fn remember_group(mark: &Path, pages: &[(&str, u64, &[u8])], leftover: &[(String, u64, Vec<u8>)]) {
    let Some(dir) = guard_live_dir(mark) else {
        return;
    };
    let mut by_file: HashMap<String, HashSet<u64>> = HashMap::new();
    for (file, offset, _) in leftover {
        by_file.entry(file.clone()).or_default().insert(*offset);
    }
    for &(file, offset, _) in pages {
        by_file.entry(file.to_string()).or_default().insert(offset);
    }
    // A new arm owns these files now: a later sync must not release it.
    let mut pending = pending_release_map()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let mut map = armed_map().lock().unwrap_or_else(|e| e.into_inner());
    for (file, offs) in by_file {
        let path = dir.join(file);
        for set in pending.values_mut() {
            set.remove(&path);
        }
        map.insert(path, offs);
    }
}

fn forget_armed(path: &Path) {
    armed_map()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path);
}

/// An arm with no pages: [`check_armed_write`] refuses every write below
/// the committed end. A file stays here until a clear or an arm of it
/// succeeds.
fn block_armed(path: &Path) {
    armed_map()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .insert(path.to_path_buf(), HashSet::new());
}

/// Live paths of `names` under the store that holds `mark`.
fn live_paths(mark: &Path, names: &HashSet<String>) -> Vec<PathBuf> {
    let Some(dir) = guard_live_dir(mark) else {
        return Vec::new();
    };
    names.iter().map(|name| dir.join(name)).collect()
}

/// Sync the guard directory after a mark change that drops `released`.
/// A file is released only once that change is durable. Until then a crash
/// can bring the old mark back, and open would copy its images over a later
/// write, so a failed sync blocks the file and keeps it pending. A later
/// sync of the same directory releases every pending file with `released`.
fn sync_mark_dir_then_release(parent: &Path, released: &[PathBuf]) -> Result<(), StoreError> {
    let synced = sync_mark_dir(parent);
    let mut pending = pending_release_map()
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if synced.is_ok() {
        let earlier = pending.remove(parent).unwrap_or_default();
        for path in earlier.iter().chain(released) {
            forget_armed(path);
        }
    } else {
        hold_pending(&mut pending, parent, released);
    }
    synced
}

/// Block `released` and keep them pending on `parent` until a sync of it
/// succeeds.
fn hold_pending(
    pending: &mut HashMap<PathBuf, HashSet<PathBuf>>,
    parent: &Path,
    released: &[PathBuf],
) {
    let set = pending.entry(parent.to_path_buf()).or_default();
    for path in released {
        block_armed(path);
        set.insert(path.clone());
    }
}

/// Reject a range that shares a committed page when `path` has a live arm
/// and the range is not inside the pages that arm names.
///
/// An append (every byte at or past the committed end) is allowed. Any other
/// write to a file with a live arm must be inside the pages that arm names:
/// even a one-sector write that lands whole may belong to the commit the
/// mark protects, and open would roll the mark back around it. A path with
/// no live arm is not checked. A file whose mark change failed its directory
/// sync refuses every such write. `committed_end` is the durable header
/// length, not a RAM high-water.
#[doc(hidden)]
pub fn check_armed_write(
    path: &Path,
    committed_end: u64,
    offset: u64,
    len: u64,
) -> Result<(), StoreError> {
    if len == 0 || offset >= committed_end {
        return Ok(());
    }
    let map = armed_map().lock().unwrap_or_else(|e| e.into_inner());
    let Some(pages) = map.get(path) else {
        return Ok(());
    };
    if pages.is_empty() {
        return Err(StoreError::Rejected(
            "a guard mark change for this file is not durable",
        ));
    }
    let Some(end) = offset.checked_add(len) else {
        return Err(StoreError::Rejected("write is outside the armed pages"));
    };
    let mut page = offset & !(PAGE - 1);
    let last = end.saturating_sub(1) & !(PAGE - 1);
    loop {
        if !pages.contains(&page) {
            return Err(StoreError::Rejected("write is outside the armed pages"));
        }
        if page >= last {
            break;
        }
        page = page.saturating_add(PAGE);
    }
    Ok(())
}

/// One process-wide hold for every mark. Arm and clear of the same path must
/// sit inside one hold, or a second flusher can rename the file between them.
#[doc(hidden)]
pub fn hold_marks() -> MutexGuard<'static, ()> {
    static MARKS: OnceLock<Mutex<()>> = OnceLock::new();
    MARKS
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// One group mark for several files. A retry replaces the image at the same
/// file and offset. Other offsets stay, including other pages of a file this
/// set names.
///
/// Restore copies each image verbatim and does not check its kind. Pre-images
/// make a backward mark (a crash rolls back); finished pages make a forward
/// mark (a crash completes the write). The caller picks and documents it.
#[doc(hidden)]
pub fn arm_page_guard_group(path: &Path, pages: &[(&str, u64, &[u8])]) -> Result<(), StoreError> {
    ensure_mark_parent(path)?;
    if pages.is_empty() {
        return Err(StoreError::Corrupt("page guard arm is empty"));
    }
    let mut seen: HashSet<(&str, u64)> = HashSet::with_capacity(pages.len());
    for &(file, offset, image) in pages {
        if !valid_guard_name(file) {
            return Err(StoreError::Corrupt("page guard file name"));
        }
        if !offset.is_multiple_of(PAGE) || image.len() != PAGE as usize {
            return Err(StoreError::Corrupt("page guard image is not one page"));
        }
        if !seen.insert((file, offset)) {
            return Err(StoreError::Corrupt("page guard names one page twice"));
        }
    }
    let leftover = load_group_leftover(path, &seen)?;
    let count = u32::try_from(pages.len().saturating_add(leftover.len()))
        .map_err(|_| StoreError::Corrupt("page guard arm is too large"))?;
    let mut buf = Vec::with_capacity(HEAD + (pages.len() + leftover.len()) * (2 + 16 + RECORD));
    buf.extend_from_slice(&STORE_MAGIC);
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf.extend_from_slice(&GROUP_KIND.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    for (file, offset, image) in &leftover {
        encode_group_record(&mut buf, file, *offset, image)?;
    }
    for &(file, offset, image) in pages {
        encode_group_record(&mut buf, file, offset, image)?;
    }
    // The rename makes the mark visible, so register its pages before the
    // directory sync. A failed sync then leaves the check matching the mark.
    write_file_synced_tmp_rename(path, &buf)?;
    remember_group(path, pages, &leftover);
    #[cfg(debug_assertions)]
    note_armed(path);
    sync_arm_dir(mark_parent(path)?)
}

/// Copies every named image onto its file under `dir`. Does not read live bytes.
///
/// `Ok(false)` is a missing mark. A mark that does not parse is an error, and
/// the live files are left alone.
#[doc(hidden)]
pub fn restore_page_guard_group(mark: &Path, dir: &Path) -> Result<bool, StoreError> {
    let Some(restored) = replay_page_guard_group(mark, dir)? else {
        // A retry after a failed sync finds no mark; its sync releases the
        // files that unlink left pending. No `guard/` means no mark change
        // to make durable.
        let parent = mark_parent(mark)?;
        if parent.is_dir() {
            sync_mark_dir_then_release(parent, &[])?;
        }
        return Ok(false);
    };
    remove_mark(mark, &restored)?;
    Ok(true)
}

/// Copies the images and leaves the mark. They are on the device when this
/// returns. `Some` names the live files it wrote; `None` is a missing mark.
fn replay_page_guard_group(mark: &Path, dir: &Path) -> Result<Option<Vec<PathBuf>>, StoreError> {
    mark_parent(mark)?;
    let raw = match std::fs::read(mark) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(StoreError::io(mark, e)),
    };
    let pages = mark_group_pages(&raw)?;
    let mut opened: Vec<(String, File, PathBuf)> = Vec::new();
    for (name, offset, at) in &pages {
        let data_path = dir.join(name);
        if !opened.iter().any(|(have, _, _)| have == name) {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&data_path)
                .map_err(|e| StoreError::io(&data_path, e))?;
            opened.push((name.clone(), file, data_path.clone()));
        }
        let Some((_, data, live)) = opened.iter().find(|(have, _, _)| have == name) else {
            return Err(StoreError::Corrupt("page guard file name"));
        };
        write_restored_page(data, live, *offset, &raw[*at..*at + PAGE as usize], None)?;
    }
    for (_, data, data_path) in &opened {
        data.durable_sync_data()
            .map_err(|e| StoreError::io(data_path, e))?;
    }
    // A created directory entry has to be durable before the mark is gone.
    let mut parents: Vec<&Path> = Vec::new();
    for (_, _, data_path) in &opened {
        let parent = data_path.parent().unwrap_or(dir);
        if !parents.contains(&parent) {
            fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))?;
            parents.push(parent);
        }
    }
    Ok(Some(opened.into_iter().map(|(_, _, path)| path).collect()))
}

/// Copies named images onto existing files. A missing live file is skipped.
///
/// A short file keeps its length: the sidecar pads to 4 KiB, and restore
/// writes only the bytes that were on disk. An empty live file is filled
/// with the full page.
#[doc(hidden)]
pub fn restore_page_guard_group_fit(mark: &Path, dir: &Path) -> Result<bool, StoreError> {
    let parent = mark_parent(mark)?;
    let raw = match std::fs::read(mark) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A retry after a failed sync finds no mark; its sync releases
            // the files that unlink left pending. No `guard/` means no mark
            // change to make durable.
            if parent.is_dir() {
                sync_mark_dir_then_release(parent, &[])?;
            }
            return Ok(false);
        }
        Err(e) => return Err(StoreError::io(mark, e)),
    };
    let pages = mark_group_pages(&raw)?;
    // A forward `*.ahead` image is the post-commit page. The backward mark
    // wins: drop the sidecar before any head open can replay it.
    let mut dropped_ahead: Vec<String> = Vec::new();
    for (name, _, _) in &pages {
        if dropped_ahead.iter().any(|have| have == name) {
            continue;
        }
        drop_ahead_sidecar(dir, name)?;
        dropped_ahead.push(name.clone());
    }
    let mut opened: Vec<(String, File, PathBuf, u64)> = Vec::new();
    for (name, offset, at) in &pages {
        let data_path = dir.join(name);
        if !opened.iter().any(|(have, _, _, _)| have == name) {
            let file = match OpenOptions::new().read(true).write(true).open(&data_path) {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(StoreError::io(&data_path, e)),
            };
            let len = file
                .metadata()
                .map_err(|e| StoreError::io(&data_path, e))?
                .len();
            if len == 0 {
                fit_empty_to_image(&file, &data_path, name, &pages)?;
            }
            opened.push((name.clone(), file, data_path.clone(), len));
        }
        let Some((_, data, live, len)) = opened.iter().find(|(have, _, _, _)| have == name) else {
            continue;
        };
        let image = &raw[*at..*at + PAGE as usize];
        write_restored_page(data, live, *offset, image, Some(*len))?;
    }
    for (_, data, data_path, _) in &opened {
        data.durable_sync_data()
            .map_err(|e| StoreError::io(data_path, e))?;
    }
    let mut names = HashSet::new();
    for (name, _, _) in &pages {
        names.insert(name.clone());
    }
    remove_mark(mark, &live_paths(mark, &names))?;
    Ok(true)
}

/// Give an empty live file the full length of its images, durably, before
/// any page lands. A restore that stops partway then leaves a file a retry
/// sees as that length, so the retry writes every page in full.
fn fit_empty_to_image(
    file: &File,
    path: &Path,
    name: &str,
    pages: &[(String, u64, usize)],
) -> Result<(), StoreError> {
    let end = pages
        .iter()
        .filter(|(have, _, _)| have == name)
        .map(|(_, offset, _)| offset.saturating_add(PAGE))
        .max()
        .unwrap_or(0);
    crate::file::set_file_len(file, path, end)?;
    file.durable_sync_data()
        .map_err(|e| StoreError::io(path, e))
}

/// Syncs `data`, then removes `path` and syncs its directory.
///
/// A path that is already gone is `Ok` after the sync. A mark with no
/// directory is an error, so a relative name cannot skip that sync.
#[doc(hidden)]
pub fn clear_page_guard(path: &Path, data: &File, data_path: &Path) -> Result<(), StoreError> {
    clear_page_guard_files(path, &[(data, data_path)])
}

/// Syncs every live file, then drops those files' images.
///
/// Records for a file this call did not sync stay armed. The mark file goes
/// away only when nothing remains.
#[doc(hidden)]
pub fn clear_page_guard_files(path: &Path, files: &[(&File, &Path)]) -> Result<(), StoreError> {
    mark_parent(path)?;
    for (data, data_path) in files {
        #[cfg(feature = "write-probe")]
        let probe_n = crate::write_probe::sync_begin(data_path, Some(data));
        data.durable_sync_data()
            .map_err(|e| StoreError::io(*data_path, e))?;
        #[cfg(feature = "write-probe")]
        crate::write_probe::synced(data_path, probe_n);
    }
    let mut synced = HashSet::with_capacity(files.len());
    for (_, data_path) in files {
        synced.insert(guard_file_name(data_path)?.to_string());
    }
    retain_unsynced(path, &synced)?;
    for (_, data_path) in files {
        forget_armed(data_path);
    }
    Ok(())
}

/// Rewrite `path` without records for `synced`. Delete it when none remain.
fn retain_unsynced(path: &Path, synced: &HashSet<String>) -> Result<(), StoreError> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // A retry after a failed sync finds no mark; its sync releases
            // the files that change left pending.
            return sync_mark_dir_then_release(mark_parent(path)?, &[]);
        }
        Err(e) => return Err(StoreError::io(path, e)),
    };
    // A mark of a kind this binary does not know stays for the restore to
    // refuse.
    let pages = mark_group_pages(&raw)?;
    let mut keep = Vec::new();
    let mut dropped = HashSet::new();
    for (name, offset, at) in pages {
        if synced.contains(&name) {
            dropped.insert(name);
            continue;
        }
        keep.push((name, offset, raw[at..at + PAGE as usize].to_vec()));
    }
    let released = live_paths(path, &dropped);
    if keep.is_empty() {
        return remove_mark(path, &released);
    }
    let count = u32::try_from(keep.len())
        .map_err(|_| StoreError::Corrupt("page guard arm is too large"))?;
    let mut buf = Vec::with_capacity(HEAD + keep.len() * (2 + 16 + RECORD));
    buf.extend_from_slice(&STORE_MAGIC);
    buf.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    buf.extend_from_slice(&GROUP_KIND.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    for (file, offset, image) in &keep {
        encode_group_record(&mut buf, file, *offset, image)?;
    }
    write_file_synced_tmp_rename(path, &buf)?;
    sync_mark_dir_then_release(mark_parent(path)?, &released)
}

/// Restore `dir/<mark>` when it exists, then continue. Missing is `Ok`.
#[doc(hidden)]
pub fn restore_store_group(dir: &Path, mark: &str) -> Result<bool, StoreError> {
    restore_page_guard_group(&dir.join(mark), dir)
}

/// Publish `dir/<mark>` as an empty flag.
///
/// A flag names no pages. Its presence alone tells open how to read the
/// files, so a crash leaves it or not, and nothing is copied back.
pub(crate) fn arm_store_flag(dir: &Path, mark: &str) -> Result<(), StoreError> {
    let path = dir.join(mark);
    ensure_mark_parent(&path)?;
    crate::file::write_file_synced_tmp_rename(&path, &[])?;
    sync_arm_dir(mark_parent(&path)?)?;
    #[cfg(debug_assertions)]
    ARM_TRACE.with(|trace| {
        if let Some(marks) = trace.borrow_mut().as_mut() {
            marks.push(mark.to_string());
        }
    });
    Ok(())
}

/// Whether the flag `dir/<mark>` is published.
pub(crate) fn store_flag_armed(dir: &Path, mark: &str) -> Result<bool, StoreError> {
    let path = dir.join(mark);
    path.try_exists().map_err(|e| StoreError::io(&path, e))
}

/// Remove the flag `dir/<mark>` and sync its directory. Missing is `Ok`.
pub(crate) fn clear_store_flag(dir: &Path, mark: &str) -> Result<(), StoreError> {
    remove_mark(&dir.join(mark), &[])
}

/// Arm a store group mark. Empty is a no-op.
#[doc(hidden)]
pub fn arm_store_group(
    dir: &Path,
    mark: &str,
    pages: &[(&str, u64, &[u8])],
) -> Result<(), StoreError> {
    if pages.is_empty() {
        return Ok(());
    }
    arm_page_guard_group(&dir.join(mark), pages)
}

#[cfg(debug_assertions)]
thread_local! {
    static ARM_TRACE: std::cell::RefCell<Option<Vec<String>>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(debug_assertions)]
fn note_armed(path: &Path) {
    let name = path.file_name().map(|n| n.to_string_lossy().into_owned());
    ARM_TRACE.with(|trace| {
        if let (Some(marks), Some(name)) = (trace.borrow_mut().as_mut(), name) {
            marks.push(format!("guard/{name}"));
        }
    });
}

/// Record the marks published on this thread, including one a later clear
/// removes before the call returns.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn arm_store_mark_trace() {
    ARM_TRACE.with(|trace| *trace.borrow_mut() = Some(Vec::new()));
}

/// Stop recording and return the marks armed since [`arm_store_mark_trace`].
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn take_armed_store_marks() -> Vec<String> {
    ARM_TRACE.with(|trace| trace.borrow_mut().take().unwrap_or_default())
}

/// `guard/sh-<shard>` for one scripthash body (`00` or `ovf`).
#[doc(hidden)]
pub fn sh_mark(shard: &str) -> String {
    format!("{SH_MARK_PREFIX}{shard}")
}

/// Copy every `guard/sh-*` mark onto its live files, then drop the marks.
#[doc(hidden)]
pub fn restore_sh_marks(dir: &Path) -> Result<(), StoreError> {
    let guard = dir.join("guard");
    let rd = match std::fs::read_dir(&guard) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(StoreError::io(&guard, e)),
    };
    let mut names: Vec<String> = Vec::new();
    for ent in rd {
        let ent = ent.map_err(|e| StoreError::io(&guard, e))?;
        let name = ent.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !published_sh_mark(name) {
            continue;
        }
        let file = match ent.file_type() {
            Ok(t) if t.is_file() => true,
            Ok(_) => false,
            Err(e) => return Err(StoreError::io(ent.path(), e)),
        };
        if file {
            names.push(name.to_string());
        }
    }
    names.sort_unstable();
    for name in names {
        restore_page_guard_group_fit(&guard.join(name), dir)?;
    }
    // Sync even when no mark was left: a retry after a failed sync finds
    // none, and the files that unlink blocked stay pending until `guard/`
    // syncs.
    sync_mark_dir_then_release(&guard, &[])
}

/// Drop every `guard/sh-*` mark. Missing is `Ok`.
#[doc(hidden)]
pub fn clear_sh_marks(dir: &Path) -> Result<(), StoreError> {
    let guard = dir.join("guard");
    let rd = match std::fs::read_dir(&guard) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(StoreError::io(&guard, e)),
    };
    // Every entry is read before the first unlink, so a directory-entry
    // error cannot follow an unlink.
    let mut names = Vec::new();
    for ent in rd {
        let ent = ent.map_err(|e| StoreError::io(&guard, e))?;
        if let Some(name) = ent.file_name().to_str() {
            if name.starts_with("sh-") {
                names.push(name.to_string());
            }
        }
    }
    names.sort_unstable();
    let mut released = Vec::new();
    for name in names {
        let path = guard.join(&name);
        let mut named = HashSet::new();
        if !name.ends_with(".tmp") {
            if let Ok(raw) = std::fs::read(&path) {
                if let Ok(pages) = mark_group_pages(&raw) {
                    for (file, _, _) in pages {
                        named.insert(file);
                    }
                }
            }
        }
        match std::fs::remove_file(&path) {
            Ok(()) => released.extend(live_paths(&path, &named)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                // The earlier unlinks are not durable, and a retry no longer
                // finds those marks: their files stay pending on `guard/`.
                hold_pending(
                    &mut pending_release_map()
                        .lock()
                        .unwrap_or_else(|e| e.into_inner()),
                    &guard,
                    &released,
                );
                return Err(StoreError::io(&path, e));
            }
        }
    }
    // Sync even when this call unlinked nothing: a retry after a failed
    // sync finds no mark, and the earlier unlink is not durable yet.
    sync_mark_dir_then_release(&guard, &released)
}

/// Sync every live file, then drop those files' relative names from `mark`.
#[doc(hidden)]
pub fn clear_page_guard_rel(
    mark: &Path,
    dir: &Path,
    files: &[(&File, &Path)],
) -> Result<(), StoreError> {
    mark_parent(mark)?;
    for (data, data_path) in files {
        #[cfg(feature = "write-probe")]
        let probe_n = crate::write_probe::sync_begin(data_path, Some(data));
        data.durable_sync_data()
            .map_err(|e| StoreError::io(*data_path, e))?;
        #[cfg(feature = "write-probe")]
        crate::write_probe::synced(data_path, probe_n);
    }
    let mut synced = HashSet::with_capacity(files.len());
    for (_, data_path) in files {
        synced.insert(guard_rel_name(dir, data_path)?);
    }
    retain_unsynced(mark, &synced)
}

/// Drop `names` from `mark` without syncing live files. Used before a rename
/// replaces a file the mark still images.
#[doc(hidden)]
pub fn drop_page_guard_names(mark: &Path, names: &HashSet<String>) -> Result<(), StoreError> {
    mark_parent(mark)?;
    retain_unsynced(mark, names)
}

/// A renamed `guard/sh-*` mark. `sh-<shard>.tmp` is a rename leftover that
/// may be partial; restore and drop skip it, and reset removes it.
pub(crate) fn published_sh_mark(name: &str) -> bool {
    name.starts_with("sh-") && !name.ends_with(".tmp")
}

/// Drop `rel` from every `guard/sh-*` mark, then sync `guard/` and the
/// directory that holds `rel`.
#[doc(hidden)]
pub fn drop_sh_rel(dir: &Path, rel: &str) -> Result<(), StoreError> {
    let guard = dir.join("guard");
    let rd = match std::fs::read_dir(&guard) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(StoreError::io(&guard, e)),
    };
    let mut names = HashSet::new();
    names.insert(rel.to_string());
    for ent in rd {
        let ent = ent.map_err(|e| StoreError::io(&guard, e))?;
        let name = ent.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if published_sh_mark(name) {
            drop_page_guard_names(&ent.path(), &names)?;
        }
    }
    // Sync even when no mark was left: a retry after a failed sync finds
    // none, and the files that unlink blocked stay pending until `guard/`
    // syncs.
    sync_mark_dir_then_release(&guard, &[])?;
    if let Some(parent) = Path::new(rel).parent() {
        if !parent.as_os_str().is_empty() {
            let live_parent = dir.join(parent);
            if live_parent.is_dir() {
                fsync_parent_dir(&live_parent).map_err(|e| StoreError::io(&live_parent, e))?;
            }
        }
    }
    Ok(())
}

/// Store-relative 1–2 component name of `path`.
#[doc(hidden)]
pub fn guard_rel_name(dir: &Path, path: &Path) -> Result<String, StoreError> {
    let rel = path
        .strip_prefix(dir)
        .map_err(|_| StoreError::Corrupt("page guard file name"))?;
    let name = rel
        .to_str()
        .ok_or(StoreError::Corrupt("page guard file name"))?
        .replace('\\', "/");
    if !valid_guard_name(&name) {
        return Err(StoreError::Corrupt("page guard file name"));
    }
    Ok(name)
}

/// Remove `{file}.ahead` next to a file the mark is about to restore.
fn drop_ahead_sidecar(dir: &Path, name: &str) -> Result<(), StoreError> {
    let live = dir.join(name);
    let Some(file_name) = live.file_name() else {
        return Ok(());
    };
    let mut ahead = file_name.to_os_string();
    ahead.push(".ahead");
    let side = match live.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(ahead),
        _ => PathBuf::from(ahead),
    };
    match std::fs::remove_file(&side) {
        Ok(()) => {}
        // An earlier unlink may have landed and its directory sync failed.
        // Sync again so the sidecar cannot come back after the mark is gone.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::io(&side, e)),
    }
    if let Some(parent) = side.parent().filter(|p| p.is_dir()) {
        fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    Ok(())
}

/// Read one 4 KiB block. Bytes past EOF are zeros. A missing file is zeros.
#[doc(hidden)]
pub fn read_live_page(
    path: &Path,
    file: &File,
    block: u64,
) -> Result<[u8; PAGE as usize], StoreError> {
    let mut image = [0u8; PAGE as usize];
    let len = match std::fs::metadata(path) {
        Ok(m) => m.len(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(image),
        Err(e) => return Err(StoreError::io(path, e)),
    };
    if block < len {
        let n = ((len - block) as usize).min(PAGE as usize);
        handle_pread_all(file, path, block, &mut image[..n])?;
    }
    Ok(image)
}

/// File name of `path` as a single component, or `Corrupt`.
#[doc(hidden)]
pub fn guard_file_name(path: &Path) -> Result<&str, StoreError> {
    path.file_name()
        .and_then(|n| n.to_str())
        .filter(|n| !n.is_empty() && !n.contains('/') && !n.contains('\\'))
        .ok_or(StoreError::Corrupt("page guard file name"))
}

fn encode_group_record(
    buf: &mut Vec<u8>,
    file: &str,
    offset: u64,
    image: &[u8],
) -> Result<(), StoreError> {
    let name_len =
        u16::try_from(file.len()).map_err(|_| StoreError::Corrupt("page guard file name"))?;
    buf.extend_from_slice(&name_len.to_le_bytes());
    buf.extend_from_slice(file.as_bytes());
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(image);
    Ok(())
}

fn load_group_leftover(
    path: &Path,
    replaced: &HashSet<(&str, u64)>,
) -> Result<Vec<(String, u64, Vec<u8>)>, StoreError> {
    let raw = match std::fs::read(path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(StoreError::io(path, e)),
    };
    let pages = mark_group_pages(&raw)?;
    let mut leftover = Vec::new();
    for (name, offset, at) in pages {
        if replaced.contains(&(name.as_str(), offset)) {
            continue;
        }
        leftover.push((name, offset, raw[at..at + PAGE as usize].to_vec()));
    }
    Ok(leftover)
}

/// The (file, page offset) records `mark` holds. A missing mark holds none.
pub(crate) fn armed_group_pages(mark: &Path) -> Result<HashSet<(String, u64)>, StoreError> {
    let raw = match std::fs::read(mark) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(HashSet::new()),
        Err(e) => return Err(StoreError::io(mark, e)),
    };
    Ok(mark_group_pages(&raw)?
        .into_iter()
        .map(|(name, offset, _)| (name, offset))
        .collect())
}

/// One or two `/`-separated parts, each a plain name. A name has one
/// spelling: `Path` would read `sub//live` and `sub/live/` as `sub/live`,
/// and a retry and the parser compare the raw text. No `:`: on Windows
/// `C:live` is a drive prefix, and `Path::join` would leave the store.
fn valid_guard_name(name: &str) -> bool {
    if name.len() > u16::MAX as usize
        || name.contains('\\')
        || name.contains('\0')
        || name.contains(':')
    {
        return false;
    }
    let mut n = 0usize;
    for part in name.split('/') {
        if part.is_empty() || part == "." || part == ".." {
            return false;
        }
        n += 1;
    }
    n == 1 || n == 2
}

/// `fit_len` is the live length read before this restore's first write.
/// `None` or an empty file takes the whole image.
fn write_restored_page(
    data: &File,
    live: &Path,
    offset: u64,
    image: &[u8],
    fit_len: Option<u64>,
) -> Result<(), StoreError> {
    let len = fit_len.unwrap_or(0);
    if len == 0 {
        return handle_pwrite_all(data, live, offset, image);
    }
    if offset >= len {
        return Ok(());
    }
    let n = ((len - offset) as usize).min(image.len());
    handle_pwrite_all(data, live, offset, &image[..n])
}

fn mark_parent(path: &Path) -> Result<&Path, StoreError> {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => Ok(parent),
        _ => Err(StoreError::Rejected("page guard mark has no directory")),
    }
}

/// Create `path`'s directory and sync that directory's parent.
///
/// The mark rename syncs the guard directory. The store directory still has
/// to name `guard/` before a crash, or the mark never comes back. A
/// `guard/` that already exists is synced again, because an earlier arm may
/// have created it and then failed that sync. A mark with no store
/// directory above `guard/` is refused before anything is created: the
/// write check keys its pages by that directory. So is a mark outside
/// `guard/` or one that ends in `.tmp`: open looks for marks only in
/// `guard/` and skips the staging names there, and could replace a store
/// whose mark it never saw.
fn ensure_mark_parent(path: &Path) -> Result<(), StoreError> {
    let parent = mark_parent(path)?;
    if parent.file_name().and_then(|n| n.to_str()) != Some(GUARD_DIR) {
        return Err(StoreError::Rejected("page guard mark is not in guard/"));
    }
    if path
        .file_name()
        .and_then(|n| n.to_str())
        .is_none_or(|n| n.ends_with(".tmp"))
    {
        return Err(StoreError::Rejected("page guard mark name"));
    }
    let Some(store) = guard_live_dir(path) else {
        return Err(StoreError::Rejected(
            "page guard mark has no store directory",
        ));
    };
    if !parent.is_dir() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    fsync_parent_dir(store).map_err(|e| StoreError::io(store, e))
}

/// Unlink `path` and sync its directory, then release `released`. A failed
/// sync blocks them: the unlink is not durable, and neither the old pages
/// nor an unchecked file may take writes the old mark would roll back.
fn remove_mark(path: &Path, released: &[PathBuf]) -> Result<(), StoreError> {
    let parent = mark_parent(path)?;
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::io(path, e)),
    }
    sync_mark_dir_then_release(parent, released)
}

/// Sync the guard directory after a mark was unlinked or renamed over.
fn sync_mark_dir(parent: &Path) -> Result<(), StoreError> {
    #[cfg(debug_assertions)]
    if take_fail_dir_sync(&FAIL_MARK_DIR_SYNC, parent) {
        return Err(injected_dir_sync_failure(parent));
    }
    fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))
}

/// Sync the guard directory after an arm renamed its mark into place.
fn sync_arm_dir(parent: &Path) -> Result<(), StoreError> {
    #[cfg(debug_assertions)]
    if take_fail_dir_sync(&FAIL_ARM_DIR_SYNC, parent) {
        return Err(injected_dir_sync_failure(parent));
    }
    fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))
}

#[cfg(debug_assertions)]
fn injected_dir_sync_failure(parent: &Path) -> StoreError {
    StoreError::io(
        parent,
        std::io::Error::other("injected guard directory sync failure"),
    )
}

#[cfg(debug_assertions)]
static FAIL_MARK_DIR_SYNC: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

#[cfg(debug_assertions)]
static FAIL_ARM_DIR_SYNC: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

/// The next arm in guard directory `dir` renames its mark into place and
/// then fails its directory sync.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_arm_dir_sync(dir: &Path) {
    FAIL_ARM_DIR_SYNC
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(dir.to_path_buf());
}

/// The next mark removal or rewrite in guard directory `dir` unlinks or
/// renames the mark and then fails its directory sync.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn fail_next_mark_dir_sync(dir: &Path) {
    FAIL_MARK_DIR_SYNC
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .push(dir.to_path_buf());
}

#[cfg(debug_assertions)]
fn take_fail_dir_sync(list: &Mutex<Vec<PathBuf>>, dir: &Path) -> bool {
    let mut armed = list.lock().unwrap_or_else(|e| e.into_inner());
    let Some(i) = armed.iter().position(|p| p == dir) else {
        return false;
    };
    armed.remove(i);
    true
}

/// Writes `bytes` at `offset` when every byte is at or past `committed_end`.
/// The write may share the committed tail block: those committed sectors go
/// back to the device unchanged, so a tear cannot reach them. An empty write
/// is `Ok`.
#[doc(hidden)]
pub fn append_past_end(
    file: &File,
    path: &Path,
    committed_end: u64,
    offset: u64,
    bytes: &[u8],
) -> Result<(), StoreError> {
    if bytes.is_empty() {
        return Ok(());
    }
    if offset < committed_end {
        return Err(StoreError::Rejected("append overwrites committed bytes"));
    }
    handle_pwrite_all(file, path, offset, bytes)
}

/// One file's part of [`commit_appends`].
///
/// `bytes` go at `at`, which is at or past the file's committed end.
/// `new_logical` is the length the leading header names afterwards.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct Append<'a> {
    pub file: &'a TableFile,
    pub at: u64,
    pub bytes: &'a [u8],
    pub new_logical: u64,
    /// Length-publish wave. Wave 0 is written and synced before wave 1 is
    /// written. Files that share a wave share one sync.
    pub length_wave: u32,
    /// Order inside one wave. A lower order is synced before a higher
    /// order's lengths are written. A segment index uses its file id, so
    /// the earlier pair's index length is durable before the next pair's
    /// index length is written.
    pub length_order: u32,
}

/// Publish a group of appends with no mark, under the sector rule.
///
/// Every file's bytes are written past its published end and synced
/// together. Lengths then go out one [`Append::length_wave`] at a time:
/// that wave's lengths are written and synced before the next wave's
/// lengths are written. A body at wave 0 and its index at wave 1 is
/// durable, bytes and length, before the index length that names it.
/// Files that share a wave and a [`Append::length_order`] share the sync.
/// A lower order in that wave is synced before a higher order's lengths
/// are written, so a later segment index cannot be published while the
/// earlier segment's index length is still old. A crash leaves each file
/// at its old length or its new one, and bytes past a length are not read.
/// A format whose last record ends at the body length must fit that
/// record on open, as `sp_tweaks` does. On an error a length may already
/// be durable, so every file in the group is poisoned: it refuses writes
/// until reopen fits the group.
#[doc(hidden)]
pub fn commit_appends(writes: &[Append<'_>]) -> Result<(), StoreError> {
    let committed = commit_appends_once(writes);
    if committed.is_err() {
        for w in writes {
            w.file.poison();
        }
    }
    committed
}

fn commit_appends_once(writes: &[Append<'_>]) -> Result<(), StoreError> {
    // Refuse before any write, so a retry cannot stack on the group.
    for w in writes {
        w.file.refuse_if_poisoned()?;
    }
    struct Published<'a> {
        file: &'a TableFile,
        end: u64,
        wave: u32,
        order: u32,
    }
    let mut files: Vec<Published<'_>> = Vec::new();
    for w in writes {
        if w.file.trailing_header {
            return Err(StoreError::Corrupt("an append needs a leading-header file"));
        }
        if w.at < w.file.logical_len() {
            return Err(StoreError::Rejected("append overwrites committed bytes"));
        }
        w.file.write_uncommitted(w.at, w.bytes)?;
        match files.iter_mut().find(|f| f.file.path() == w.file.path()) {
            Some(got) => {
                if got.wave != w.length_wave {
                    return Err(StoreError::Corrupt("an append file has two length waves"));
                }
                if got.order != w.length_order {
                    return Err(StoreError::Corrupt("an append file has two length orders"));
                }
                got.end = got.end.max(w.new_logical);
            }
            None => files.push(Published {
                file: w.file,
                end: w.new_logical,
                wave: w.length_wave,
                order: w.length_order,
            }),
        }
    }
    if !files.is_empty() {
        let refs: Vec<&TableFile> = files.iter().map(|f| f.file).collect();
        TableFile::sync_data_all(&refs)?;
    }
    #[cfg(debug_assertions)]
    run_commit_hook()?;
    let mut waves: Vec<u32> = files.iter().map(|f| f.wave).collect();
    waves.sort_unstable();
    waves.dedup();
    for wave in waves {
        let mut orders: Vec<u32> = files
            .iter()
            .filter(|f| f.wave == wave)
            .map(|f| f.order)
            .collect();
        orders.sort_unstable();
        orders.dedup();
        for order in orders {
            let group: Vec<&TableFile> = files
                .iter()
                .filter(|f| f.wave == wave && f.order == order)
                .map(|f| f.file)
                .collect();
            for f in files.iter().filter(|f| f.wave == wave && f.order == order) {
                f.file.set_logical_len(f.end)?;
            }
            TableFile::sync_length_wave(&group)?;
        }
    }
    Ok(())
}

fn mark_group_pages(raw: &[u8]) -> Result<Vec<(String, u64, usize)>, StoreError> {
    if raw.len() < HEAD || raw[..4] != STORE_MAGIC {
        return Err(StoreError::Corrupt("page guard mark is not a guard file"));
    }
    let schema = u16::from_le_bytes([raw[4], raw[5]]);
    if !schema_file_openable(schema) {
        return Err(StoreError::Corrupt("page guard mark schema"));
    }
    let kind = u16::from_le_bytes([raw[6], raw[7]]);
    if kind != GROUP_KIND {
        return Err(StoreError::Corrupt("page guard mark reserved"));
    }
    let count = u32::from_le_bytes([raw[8], raw[9], raw[10], raw[11]]) as usize;
    if count == 0 {
        return Err(StoreError::Corrupt("page guard mark length"));
    }
    // One record is at least a 1-byte name, its length, the offset, and a page.
    const MIN_RECORD: usize = 2 + 1 + 8 + PAGE as usize;
    let rest = raw.len().saturating_sub(HEAD);
    if rest / MIN_RECORD < count {
        return Err(StoreError::Corrupt("page guard mark length"));
    }
    let mut pages = Vec::with_capacity(count);
    let mut seen: HashSet<(String, u64)> = HashSet::with_capacity(count);
    let mut at = HEAD;
    for _ in 0..count {
        if raw.len() < at + 2 {
            return Err(StoreError::Corrupt("page guard mark length"));
        }
        let name_len = u16::from_le_bytes([raw[at], raw[at + 1]]) as usize;
        at += 2;
        if name_len == 0 || raw.len() < at + name_len + 8 + PAGE as usize {
            return Err(StoreError::Corrupt("page guard mark length"));
        }
        let name = std::str::from_utf8(&raw[at..at + name_len])
            .map_err(|_| StoreError::Corrupt("page guard file name"))?;
        if !valid_guard_name(name) {
            return Err(StoreError::Corrupt("page guard file name"));
        }
        at += name_len;
        let mut off = [0u8; 8];
        off.copy_from_slice(&raw[at..at + 8]);
        let offset = u64::from_le_bytes(off);
        at += 8;
        if !offset.is_multiple_of(PAGE) || !seen.insert((name.to_string(), offset)) {
            return Err(StoreError::Corrupt("page guard names a torn page"));
        }
        pages.push((name.to_string(), offset, at));
        at += PAGE as usize;
    }
    if at != raw.len() {
        return Err(StoreError::Corrupt("page guard mark length"));
    }
    Ok(pages)
}

#[cfg(debug_assertions)]
type CommitHook = Box<dyn FnOnce() -> Result<(), StoreError>>;

#[cfg(debug_assertions)]
thread_local! {
    static COMMIT_HOOK: std::cell::RefCell<Option<CommitHook>> =
        const { std::cell::RefCell::new(None) };
}

/// Run `hook` once on this thread inside the next [`commit_appends`], after
/// its bytes are synced and before any length moves. An error fails that
/// commit there.
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_commit_hook(hook: Option<CommitHook>) {
    COMMIT_HOOK.with(|slot| *slot.borrow_mut() = hook);
}

#[cfg(debug_assertions)]
fn run_commit_hook() -> Result<(), StoreError> {
    match COMMIT_HOOK.with(|slot| slot.borrow_mut().take()) {
        Some(hook) => hook(),
        None => Ok(()),
    }
}

/// Write probe: `path` has an arm whose pages cover the whole write.
#[cfg(feature = "write-probe")]
pub(crate) fn probe_armed_covers(path: &Path, offset: u64, len: u64) -> bool {
    let map = armed_map().lock().unwrap_or_else(|e| e.into_inner());
    let Some(pages) = map.get(path) else {
        return false;
    };
    let mut page = offset & !(PAGE - 1);
    while page < offset + len {
        if !pages.contains(&page) {
            return false;
        }
        page += PAGE;
    }
    true
}

/// Write probe: `path` has an arm that images at least one page of the range.
#[cfg(feature = "write-probe")]
pub(crate) fn probe_armed_touches(path: &Path, offset: u64, len: u64) -> bool {
    if len == 0 {
        return false;
    }
    let map = armed_map().lock().unwrap_or_else(|e| e.into_inner());
    let Some(pages) = map.get(path) else {
        return false;
    };
    let mut page = offset & !(PAGE - 1);
    while page < offset + len {
        if pages.contains(&page) {
            return true;
        }
        page += PAGE;
    }
    false
}
