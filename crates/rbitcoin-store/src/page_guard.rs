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
use std::path::{Component, Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use rbitcoin_primitives::{schema_file_openable, SCHEMA_VERSION, STORE_MAGIC};

use crate::durability::Durable;
use crate::error::StoreError;
use crate::file::{
    fsync_parent_dir, handle_pwrite_all, write_file_synced_tmp_rename, write_synced_tmp_rename,
};

pub const PAGE: u64 = crate::tx_table::BODY_PAGE_SIZE;
const HEAD: usize = 12;
const RECORD: usize = 8 + PAGE as usize;
const GROUP_KIND: u16 = 1;

fn armed_map() -> &'static Mutex<HashMap<PathBuf, HashSet<u64>>> {
    static ARMED: OnceLock<Mutex<HashMap<PathBuf, HashSet<u64>>>> = OnceLock::new();
    ARMED.get_or_init(|| Mutex::new(HashMap::new()))
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
    let mut map = armed_map().lock().unwrap_or_else(|e| e.into_inner());
    for (file, offs) in by_file {
        map.insert(dir.join(file), offs);
    }
}

fn forget_armed(path: &Path) {
    armed_map()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(path);
}

/// A file dropped from a mark is not armed. The release check reads this map.
fn forget_armed_names(mark: &Path, names: &HashSet<String>) {
    let Some(dir) = guard_live_dir(mark) else {
        return;
    };
    for name in names {
        forget_armed(&dir.join(name));
    }
}

/// Reject a range that shares a committed page when `path` has a live arm
/// and the range is not inside the pages that arm names.
///
/// An append (every byte at or past the committed end) is allowed. Any other
/// write to a file with a live arm must be inside the pages that arm names:
/// even a one-sector write that lands whole may belong to the commit the
/// mark protects, and open would roll the mark back around it. A path with
/// no live arm is not checked. `committed_end` is the durable header length,
/// not a RAM high-water.
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

/// One group mark for several files. A retry replaces the image at the same
/// file and offset. Other offsets stay, including other pages of a file this
/// set names.
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
    write_synced_tmp_rename(path, &buf)?;
    remember_group(path, pages, &leftover);
    Ok(())
}

/// Copies every named image onto its file under `dir`. Does not read live bytes.
///
/// `Ok(false)` is a missing mark. A mark that does not parse is an error, and
/// the live files are left alone.
#[doc(hidden)]
pub fn restore_page_guard_group(mark: &Path, dir: &Path) -> Result<bool, StoreError> {
    if !replay_page_guard_group(mark, dir)? {
        return Ok(false);
    }
    remove_mark(mark, || {})?;
    Ok(true)
}

/// Copies the images and leaves the mark. They are on the device when this
/// returns.
fn replay_page_guard_group(mark: &Path, dir: &Path) -> Result<bool, StoreError> {
    mark_parent(mark)?;
    let raw = match std::fs::read(mark) {
        Ok(raw) => raw,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
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
    for (_, _, data_path) in &opened {
        forget_armed(data_path);
    }
    Ok(true)
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
        data.durable_sync_data()
            .map_err(|e| StoreError::io(*data_path, e))?;
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
            let parent = mark_parent(path)?;
            fsync_parent_dir(parent).map_err(|err| StoreError::io(parent, err))?;
            return Ok(());
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
    if keep.is_empty() {
        return remove_mark(path, || forget_armed_names(path, &dropped));
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
    // The rename drops the names from the mark, so forget them before the
    // directory sync. A failed sync must not leave them armed.
    write_file_synced_tmp_rename(path, &buf)?;
    forget_armed_names(path, &dropped);
    sync_mark_dir(mark_parent(path)?)
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

fn valid_guard_name(name: &str) -> bool {
    if name.is_empty()
        || name.len() > u16::MAX as usize
        || name.contains('\\')
        || name.contains('\0')
    {
        return false;
    }
    let path = Path::new(name);
    if path.is_absolute() {
        return false;
    }
    let mut n = 0usize;
    for c in path.components() {
        match c {
            Component::Normal(s) => {
                let s = s.to_str().unwrap_or("");
                if s.is_empty() || s == "." || s == ".." {
                    return false;
                }
                n += 1;
                if n > 2 {
                    return false;
                }
            }
            _ => return false,
        }
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
/// have created it and then failed that sync.
fn ensure_mark_parent(path: &Path) -> Result<(), StoreError> {
    let parent = mark_parent(path)?;
    if !parent.is_dir() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    if let Some(grand) = parent.parent().filter(|p| !p.as_os_str().is_empty()) {
        fsync_parent_dir(grand).map_err(|e| StoreError::io(grand, e))?;
    }
    Ok(())
}

/// Unlink `path`, run `forget` once the mark is gone, then sync its
/// directory. A failed directory sync must not leave the release check
/// holding pages of a mark that no longer exists.
fn remove_mark(path: &Path, forget: impl FnOnce()) -> Result<(), StoreError> {
    let parent = mark_parent(path)?;
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(StoreError::io(path, e)),
    }
    forget();
    sync_mark_dir(parent)
}

/// Sync the guard directory after a mark was unlinked or renamed over.
fn sync_mark_dir(parent: &Path) -> Result<(), StoreError> {
    #[cfg(debug_assertions)]
    if take_fail_mark_dir_sync(parent) {
        return Err(StoreError::io(
            parent,
            std::io::Error::other("injected guard directory sync failure"),
        ));
    }
    fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))
}

#[cfg(debug_assertions)]
static FAIL_MARK_DIR_SYNC: Mutex<Vec<PathBuf>> = Mutex::new(Vec::new());

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
fn take_fail_mark_dir_sync(dir: &Path) -> bool {
    let mut armed = FAIL_MARK_DIR_SYNC.lock().unwrap_or_else(|e| e.into_inner());
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
