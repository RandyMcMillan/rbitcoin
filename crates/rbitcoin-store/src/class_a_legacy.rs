//! Upgrade step for a store from a build that imaged Class A pages.
//!
//! That build kept `tear_guard/first` (one sealed copy of each stem's first
//! 4 KiB), `tear_guard/tail.0` and `tail.1` (alternating copies of each
//! stem's partial last block and published length), and, before that, a
//! `CATK` `{stem}.kept`. A crash under it can leave a committed page zeroed
//! with its only synced copy here. Open picks the copies to write back the
//! way that build's open did, before it writes any stem, and keeps that
//! choice in a synced [`PLAN_NAME`]. It then writes and syncs the stems,
//! removes the plan, and removes the images. An open after a failed write
//! or sync repeats the plan, so a partly written range never reads as
//! intact. An image or plan that holds bytes but does not decode stops open
//! and stays on disk.

use crate::durability::Durable;
use crate::error::StoreError;
use crate::file::{self, FILE_HEADER_LEN};
use crate::io_handle::IoHandle;
use bitcoin_hashes::{sha256, Hash, HashEngine};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

const PAGE: usize = 4096;
const STEM_N: usize = 16;
const FIRST_BYTES: usize = STEM_N * 2 * PAGE;
const TAIL_BYTES: usize = (STEM_N + 1) * PAGE;
const CAPF: &[u8; 4] = b"CAPF";
const CAPT: &[u8; 4] = b"CAPT";
const CATK: &[u8; 4] = b"CATK";
const CATK_HEAD: usize = 16;
const VERSION: u16 = 1;
const COMMIT_BODY: usize = 40;
const COMMIT_SUM_AT: usize = 40;
const TAIL_HEADS: usize = 56;
const TAIL_LENS: usize = 312;
const CHECKSUM_AT: usize = 24;
const CHECKSUM_END: usize = 56;

const CAPR: &[u8; 4] = b"CAPR";
/// The writes a leftover image selected, kept until every one is synced.
const PLAN_NAME: &str = "tear_guard.plan";
const PLAN_TMP: &str = "tear_guard.plan.tmp";

const CORRUPT_PAGE: &str = "class A page image does not decode over a zeroed block";
const CORRUPT_IMAGE: &str = "leftover class A page image does not decode";
const CORRUPT_PLAN: &str = "class A restore plan does not decode";

/// Sync-array order of the imaging build. Slot and seal indexes use it.
const STEMS: [&str; STEM_N] = [
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

/// Put back what a leftover tear guard or `.kept` holds, then remove them.
///
/// Absent files are a no-op. A plan an earlier open left is repeated as is.
/// The plan goes only after every stem it names is synced, and the images
/// only after the plan, so a crash in between replays the same bytes or
/// selects nothing from the restored stems.
pub(crate) fn replay_and_drop(hot: &Path, seq: &Path) -> Result<(), StoreError> {
    let plan_path = hot.join(PLAN_NAME);
    let plan = match load_plan(&plan_path)? {
        Some(plan) => plan,
        None => {
            let plan = select(hot, seq)?;
            if !plan.is_empty() {
                persist_plan(hot, &plan)?;
            }
            plan
        }
    };
    apply(hot, seq, &plan)?;
    remove_file_synced(&plan_path)?;
    remove_file_synced(&hot.join(PLAN_TMP))?;
    for name in STEMS {
        remove_file_synced(&kept_side(&stem_path(hot, seq, name)))?;
    }
    let guard = hot.join("tear_guard");
    if guard.try_exists().map_err(|e| StoreError::io(&guard, e))? {
        std::fs::remove_dir_all(&guard).map_err(|e| StoreError::io(&guard, e))?;
        file::fsync_parent_dir(hot).map_err(|e| StoreError::io(hot, e))?;
    }
    Ok(())
}

fn stem_path(hot: &Path, seq: &Path, name: &str) -> PathBuf {
    if name.starts_with("seqsigwit.") || name.starts_with("txstat.") || name.starts_with("input.") {
        seq.join(name)
    } else {
        hot.join(name)
    }
}

/// One selected write: `bytes` at `off` in stem `stem`.
struct PlannedWrite {
    stem: usize,
    off: u64,
    bytes: Vec<u8>,
}

/// Every write the leftover images call for, chosen from the stems as they
/// are now. Refusals happen here, before any stem is written.
fn select(hot: &Path, seq: &Path) -> Result<Vec<PlannedWrite>, StoreError> {
    let guard = hot.join("tear_guard");
    let images = if guard.try_exists().map_err(|e| StoreError::io(&guard, e))? {
        Some((load_first(&guard.join("first"))?, load_best_tail(&guard)?))
    } else {
        None
    };
    let mut plan = Vec::new();
    for (i, name) in STEMS.iter().enumerate() {
        let path = stem_path(hot, seq, name);
        let kept = load_kept(&path)?;
        if (kept.is_none() && images.is_none())
            || !path.try_exists().map_err(|e| StoreError::io(&path, e))?
        {
            continue;
        }
        let mut stem = StemView::open(path)?;
        if let Some((block, image)) = kept {
            select_kept(&mut stem, block, &image)?;
        }
        if let Some((first, best)) = &images {
            select_guard(&mut stem, name, i, first, best.as_deref())?;
        }
        plan.extend(stem.planned.into_iter().map(|(off, bytes)| PlannedWrite {
            stem: i,
            off,
            bytes,
        }));
    }
    Ok(plan)
}

/// Write every planned range, then sync each stem it touched.
fn apply(hot: &Path, seq: &Path, plan: &[PlannedWrite]) -> Result<(), StoreError> {
    for (i, name) in STEMS.iter().enumerate() {
        let mut writes = plan.iter().filter(|w| w.stem == i).peekable();
        if writes.peek().is_none() {
            continue;
        }
        let path = stem_path(hot, seq, name);
        let file = open_rw(&path)?;
        for w in writes {
            pwrite_exact(&file, &path, w.off, &w.bytes)?;
        }
        sync_data(&file, &path)?;
    }
    Ok(())
}

/// A stem as it reads with the writes selected so far laid over it, so a
/// later choice sees what an earlier one will write.
struct StemView {
    file: File,
    path: PathBuf,
    len: u64,
    planned: Vec<(u64, Vec<u8>)>,
}

impl StemView {
    fn open(path: PathBuf) -> Result<Self, StoreError> {
        let file = open_rw(&path)?;
        let len = file.metadata().map_err(|e| StoreError::io(&path, e))?.len();
        Ok(Self {
            file,
            path,
            len,
            planned: Vec::new(),
        })
    }

    fn read(&self, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
        pread_exact(&self.file, &self.path, offset, buf)?;
        let end = offset + buf.len() as u64;
        for (at, bytes) in &self.planned {
            let lo = offset.max(*at);
            let hi = end.min(at + bytes.len() as u64);
            if lo < hi {
                buf[(lo - offset) as usize..(hi - offset) as usize]
                    .copy_from_slice(&bytes[(lo - at) as usize..(hi - at) as usize]);
            }
        }
        Ok(())
    }

    fn write(&mut self, offset: u64, bytes: &[u8]) {
        if !bytes.is_empty() {
            self.planned.push((offset, bytes.to_vec()));
        }
    }
}

// ---- tear_guard.plan ----

/// `CAPR`, a count, each write as stem, offset, length and bytes, then a
/// sha256 of everything before it.
fn encode_plan(plan: &[PlannedWrite]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(CAPR);
    buf.extend_from_slice(&(plan.len() as u32).to_le_bytes());
    for w in plan {
        buf.extend_from_slice(&(w.stem as u16).to_le_bytes());
        buf.extend_from_slice(&w.off.to_le_bytes());
        buf.extend_from_slice(&(w.bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&w.bytes);
    }
    let sum = sha256_parts(&[&buf]);
    buf.extend_from_slice(&sum);
    buf
}

fn decode_plan(raw: &[u8]) -> Option<Vec<PlannedWrite>> {
    let body_len = raw.len().checked_sub(32)?;
    let (body, sum) = raw.split_at(body_len);
    if body.len() < 8 || body[0..4] != *CAPR || sha256_parts(&[body]) != sum {
        return None;
    }
    let count = u32::from_le_bytes(body[4..8].try_into().ok()?) as usize;
    let mut at = 8;
    let mut plan = Vec::new();
    for _ in 0..count {
        let head = body.get(at..at + 14)?;
        let stem = le_u16(&head[0..2]) as usize;
        let off = le_u64(&head[2..10]);
        let n = u32::from_le_bytes(head[10..14].try_into().ok()?) as usize;
        if stem >= STEM_N || n == 0 || n > PAGE {
            return None;
        }
        let bytes = body.get(at + 14..at + 14 + n)?.to_vec();
        at += 14 + n;
        plan.push(PlannedWrite { stem, off, bytes });
    }
    (at == body.len()).then_some(plan)
}

/// The plan an earlier open left, or `None` when there is none.
fn load_plan(path: &Path) -> Result<Option<Vec<PlannedWrite>>, StoreError> {
    let raw = read_or_empty(path)?;
    if raw.is_empty() {
        return Ok(None);
    }
    decode_plan(&raw)
        .map(Some)
        .ok_or(StoreError::Corrupt(CORRUPT_PLAN))
}

/// A synced temp file renamed over the plan, so a plan is whole or absent.
fn persist_plan(hot: &Path, plan: &[PlannedWrite]) -> Result<(), StoreError> {
    let tmp = hot.join(PLAN_TMP);
    let dst = hot.join(PLAN_NAME);
    let mut file = File::create(&tmp).map_err(|e| StoreError::io(&tmp, e))?;
    file.write_all(&encode_plan(plan))
        .and_then(|()| file.durable_sync_data())
        .map_err(|e| StoreError::io(&tmp, e))?;
    drop(file);
    std::fs::rename(&tmp, &dst).map_err(|e| StoreError::io(&dst, e))?;
    file::fsync_parent_dir(hot).map_err(|e| StoreError::io(hot, e))
}

fn remove_file_synced(path: &Path) -> Result<(), StoreError> {
    match std::fs::remove_file(path) {
        Ok(()) => {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            file::fsync_parent_dir(parent).map_err(|e| StoreError::io(parent, e))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

fn read_or_empty(path: &Path) -> Result<Vec<u8>, StoreError> {
    match std::fs::read(path) {
        Ok(raw) => Ok(raw),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

fn all_zero(bytes: &[u8]) -> bool {
    !bytes.is_empty() && bytes.iter().all(|b| *b == 0)
}

fn vacant(bytes: &[u8]) -> bool {
    bytes.iter().all(|b| *b == 0)
}

fn sha256_parts(parts: &[&[u8]]) -> [u8; 32] {
    let mut eng = sha256::HashEngine::default();
    for part in parts {
        eng.input(part);
    }
    sha256::Hash::from_engine(eng).to_byte_array()
}

fn le_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(bytes);
    u64::from_le_bytes(buf)
}

fn le_u16(bytes: &[u8]) -> u16 {
    let mut buf = [0u8; 2];
    buf.copy_from_slice(bytes);
    u16::from_le_bytes(buf)
}

fn open_rw(path: &Path) -> Result<File, StoreError> {
    file::table_open_opts()
        .open(path)
        .map_err(|e| StoreError::io(path, e))
}

fn pread_exact(file: &File, path: &Path, offset: u64, buf: &mut [u8]) -> Result<(), StoreError> {
    if buf.is_empty() {
        return Ok(());
    }
    let got = IoHandle::from_file(file).pread(offset, buf);
    if got < 0 {
        return Err(StoreError::io(
            path,
            std::io::Error::from_raw_os_error(-got),
        ));
    }
    if got as usize != buf.len() {
        return Err(StoreError::Corrupt("class A page image short read"));
    }
    Ok(())
}

fn pwrite_exact(file: &File, path: &Path, offset: u64, buf: &[u8]) -> Result<(), StoreError> {
    if buf.is_empty() {
        return Ok(());
    }
    let put = IoHandle::from_file(file).pwrite(offset, buf);
    if put < 0 {
        return Err(StoreError::io(
            path,
            std::io::Error::from_raw_os_error(-put),
        ));
    }
    if put as usize != buf.len() {
        return Err(StoreError::io(
            path,
            std::io::Error::other("class A page image short write"),
        ));
    }
    Ok(())
}

fn sync_data(file: &File, path: &Path) -> Result<(), StoreError> {
    let rc = IoHandle::from_file(file).durable_sync_data();
    if rc < 0 {
        return Err(StoreError::io(path, std::io::Error::from_raw_os_error(-rc)));
    }
    Ok(())
}

// ---- {stem}.kept ----

fn kept_side(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new("file"))
        .to_os_string();
    name.push(".kept");
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(name),
        _ => PathBuf::from(name),
    }
}

fn parse_catk(raw: &[u8]) -> Option<(u64, &[u8])> {
    if raw.len() < CATK_HEAD || raw[0..4] != *CATK {
        return None;
    }
    let block = le_u64(&raw[4..12]);
    let n = u32::from_le_bytes(raw[12..16].try_into().ok()?) as usize;
    if !block.is_multiple_of(PAGE as u64) || n == 0 || n > PAGE || raw.len() != CATK_HEAD + n {
        return None;
    }
    Some((block, &raw[CATK_HEAD..]))
}

/// The `CATK` image beside `path`, if one holds bytes.
fn load_kept(path: &Path) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
    let side = kept_side(path);
    let raw = read_or_empty(&side)?;
    if vacant(&raw) {
        return Ok(None);
    }
    let Some((block, image)) = parse_catk(&raw) else {
        return Err(StoreError::Corrupt(CORRUPT_IMAGE));
    };
    Ok(Some((block, image.to_vec())))
}

/// Select a `CATK` image when its range is zeroes.
fn select_kept(stem: &mut StemView, block: u64, image: &[u8]) -> Result<(), StoreError> {
    if block.saturating_add(image.len() as u64) > stem.len {
        return Ok(());
    }
    let mut current = vec![0u8; image.len()];
    stem.read(block, &mut current)?;
    if all_zero(&current) {
        stem.write(block, image);
    }
    Ok(())
}

// ---- tear_guard/first ----

/// `first`, or empty when it holds nothing. Bytes that are not a whole
/// `first` file do not decode.
fn load_first(path: &Path) -> Result<Vec<u8>, StoreError> {
    let raw = read_or_empty(path)?;
    if raw.len() == FIRST_BYTES {
        return Ok(raw);
    }
    if vacant(&raw) {
        return Ok(Vec::new());
    }
    Err(StoreError::Corrupt(CORRUPT_IMAGE))
}

/// The sealed page 0 of `stem`. A commit page of zeroes is an unsealed
/// stem. A commit with bytes that fails its checksum does not decode.
fn sealed_page0(first: &[u8], stem: usize) -> Result<Option<Vec<u8>>, StoreError> {
    let image_at = stem * 2 * PAGE;
    let commit_at = image_at + PAGE;
    if first.len() < commit_at + PAGE {
        return Ok(None);
    }
    let commit = &first[commit_at..commit_at + PAGE];
    if vacant(commit) {
        return Ok(None);
    }
    let image = &first[image_at..image_at + PAGE];
    let decodes = commit[0..4] == *CAPF
        && le_u16(&commit[4..6]) as usize == stem
        && le_u16(&commit[6..8]) == PAGE as u16
        && sha256_parts(&[&commit[..COMMIT_BODY]]) == commit[COMMIT_SUM_AT..COMMIT_SUM_AT + 32]
        && sha256_parts(&[image]) == commit[8..40];
    if !decodes {
        return Err(StoreError::Corrupt(CORRUPT_IMAGE));
    }
    Ok(Some(image.to_vec()))
}

// ---- tear_guard/tail.{0,1} ----

struct TailSlot {
    generation: u64,
    published: [u64; STEM_N],
    tail_off: [u64; STEM_N],
    tail_len: [u16; STEM_N],
    raw: Vec<u8>,
}

enum Slot {
    /// Missing, zeroes, or a generation-0 header the build had not filled.
    Vacant,
    /// A write the crash cut short. The other slot is the copy.
    Torn,
    Live(Box<TailSlot>),
}

fn tail_checksum(buf: &[u8]) -> [u8; 32] {
    sha256_parts(&[&buf[..CHECKSUM_AT], &buf[CHECKSUM_END..]])
}

fn read_slot(path: &Path) -> Result<Slot, StoreError> {
    let raw = read_or_empty(path)?;
    if vacant(&raw) {
        return Ok(Slot::Vacant);
    }
    if raw.len() != TAIL_BYTES
        || raw[0..4] != *CAPT
        || le_u16(&raw[4..6]) != VERSION
        || le_u16(&raw[6..8]) != STEM_N as u16
        || tail_checksum(&raw) != raw[CHECKSUM_AT..CHECKSUM_END]
    {
        return Ok(Slot::Torn);
    }
    let generation = le_u64(&raw[8..16]);
    if generation == 0 {
        return Ok(Slot::Vacant);
    }
    let mut published = [0u64; STEM_N];
    let mut tail_off = [0u64; STEM_N];
    let mut tail_len = [0u16; STEM_N];
    for i in 0..STEM_N {
        let head = TAIL_HEADS + i * 16;
        published[i] = le_u64(&raw[head..head + 8]);
        tail_off[i] = le_u64(&raw[head + 8..head + 16]);
        tail_len[i] = le_u16(&raw[TAIL_LENS + i * 2..TAIL_LENS + i * 2 + 2]);
        if tail_len[i] as usize > PAGE {
            return Ok(Slot::Torn);
        }
    }
    Ok(Slot::Live(Box::new(TailSlot {
        generation,
        published,
        tail_off,
        tail_len,
        raw,
    })))
}

/// The newer live slot. The build wrote one slot at a time, so a crash
/// tears at most one: two torn slots do not decode.
fn load_best_tail(guard: &Path) -> Result<Option<Box<TailSlot>>, StoreError> {
    let slot0 = read_slot(&guard.join("tail.0"))?;
    let slot1 = read_slot(&guard.join("tail.1"))?;
    Ok(match (slot0, slot1) {
        (Slot::Live(a), Slot::Live(b)) => Some(if b.generation > a.generation { b } else { a }),
        (Slot::Live(a), _) | (_, Slot::Live(a)) => Some(a),
        (Slot::Torn, Slot::Torn) => return Err(StoreError::Corrupt(CORRUPT_IMAGE)),
        _ => None,
    })
}

fn image_of(slot: &TailSlot, stem: usize) -> &[u8] {
    let page = PAGE + stem * PAGE;
    &slot.raw[page..page + slot.tail_len[stem] as usize]
}

// ---- restore, as the imaging build's open did ----

fn tail_span(published: u64, file_len: u64) -> (u64, u16) {
    if published == 0 || published.is_multiple_of(PAGE as u64) || published > file_len {
        return (0, 0);
    }
    let off = published & !(PAGE as u64 - 1);
    let n = published - off;
    if n == 0 || n > PAGE as u64 {
        return (0, 0);
    }
    (off, n as u16)
}

fn header_published(stem: &StemView) -> Result<u64, StoreError> {
    if stem.len < FILE_HEADER_LEN as u64 {
        return Ok(0);
    }
    let mut header = [0u8; FILE_HEADER_LEN];
    stem.read(0, &mut header)?;
    Ok(le_u64(&header[8..16]))
}

/// Select page 0 and the partial last block from the guard, then refuse a
/// zeroed prefix those writes leave.
fn select_guard(
    view: &mut StemView,
    name: &str,
    stem: usize,
    first: &[u8],
    best: Option<&TailSlot>,
) -> Result<(), StoreError> {
    let file_len = view.len;
    if file_len == 0 {
        return Ok(());
    }
    let header_len = header_published(view)?;
    let mut published = best
        .map(|slot| slot.published[stem])
        .filter(|n| *n > 0)
        .unwrap_or(header_len);
    let mut page0 = vec![0u8; (file_len as usize).min(PAGE)];
    view.read(0, &mut page0)?;
    let page0_zero = all_zero(&page0);
    let sealed = sealed_page0(first, stem)?;
    let page0_restored = restore_page0(view, stem, published, sealed, best, &page0);
    if page0_restored {
        // A zeroed header read as 0. The length selected for it is the
        // one the tail check has to use.
        let restored_len = header_published(view)?;
        if restored_len > published {
            published = restored_len;
        }
    }
    let tail_restored = restore_tail_block(view, stem, published, best)?;
    refuse_zero_prefix(
        view,
        name,
        published,
        file_len,
        ZeroRefuse {
            page0_unrestored: page0_zero && !page0_restored,
            tail_restored,
            no_slot: best.is_none(),
        },
    )
}

struct ZeroRefuse {
    page0_unrestored: bool,
    tail_restored: bool,
    no_slot: bool,
}

fn restore_page0(
    view: &mut StemView,
    stem: usize,
    published: u64,
    sealed: Option<Vec<u8>>,
    best: Option<&TailSlot>,
    page0: &[u8],
) -> bool {
    if all_zero(page0) {
        if let Some(mut image) = sealed {
            let len = recorded_len(best, stem, published, le_u64(&image[8..16]));
            image[8..16].copy_from_slice(&len.to_le_bytes());
            view.write(0, &image);
            return true;
        }
    }
    let Some(slot) = best else {
        return false;
    };
    if slot.tail_off[stem] != 0 || slot.tail_len[stem] == 0 {
        return false;
    }
    let raw = image_of(slot, stem);
    let mut image = raw.to_vec();
    stamp_loaded_len(&mut image, slot.published[stem]);
    // One write. A page that is still the unstamped image is that write
    // without the slot length.
    if page_holds(page0, &image) {
        return false;
    }
    if all_zero(page0) || page_holds(page0, raw) {
        view.write(0, &image);
        return true;
    }
    false
}

fn page_holds(page: &[u8], image: &[u8]) -> bool {
    page.len() >= image.len()
        && &page[..image.len()] == image
        && page[image.len()..].iter().all(|byte| *byte == 0)
}

fn stamp_loaded_len(image: &mut [u8], published: u64) {
    if published > 0 && image.len() >= 16 {
        image[8..16].copy_from_slice(&published.to_le_bytes());
    }
}

/// A tail slot's length wins over the length bytes inside a page image.
/// Those bytes can be a later `persist_hwm` that landed during the copy.
fn recorded_len(best: Option<&TailSlot>, stem: usize, published: u64, image_len: u64) -> u64 {
    if let Some(slot) = best {
        if slot.published[stem] > 0 {
            return slot.published[stem];
        }
    }
    published.max(image_len)
}

fn restore_tail_block(
    view: &mut StemView,
    stem: usize,
    published: u64,
    best: Option<&TailSlot>,
) -> Result<bool, StoreError> {
    let (off, n) = tail_span(published, view.len);
    if n == 0 || off < PAGE as u64 {
        return Ok(false);
    }
    let mut prefix = vec![0u8; n as usize];
    view.read(off, &mut prefix)?;
    if !all_zero(&prefix) {
        return Ok(false);
    }
    let Some(slot) = best else {
        return Ok(false);
    };
    if slot.tail_off[stem] != off || slot.tail_len[stem] < n {
        return Ok(false);
    }
    let image = image_of(slot, stem);
    let take = (n as usize).min(image.len());
    view.write(off, &image[..take]);
    Ok(true)
}

/// A coinbase edge, an unstamped txstat cell, an empty seqsigwit pad, and an
/// unspent slot are zero bytes. With no tail slot, that prefix is the data.
fn zero_tail_is_record(name: &str) -> bool {
    name == "input.body"
        || name == "seqsigwit.body"
        || name == "spent.body"
        || name.starts_with("txstat.")
}

/// Stop when a zeroed prefix had no usable image to put back.
fn refuse_zero_prefix(
    view: &StemView,
    name: &str,
    published: u64,
    file_len: u64,
    flags: ZeroRefuse,
) -> Result<(), StoreError> {
    if published == 0 || file_len == 0 {
        return Ok(());
    }
    if flags.page0_unrestored {
        let page_n = (file_len as usize).min(PAGE).min(published as usize);
        let mut page0 = vec![0u8; page_n];
        view.read(0, &mut page0)?;
        if all_zero(&page0) {
            return Err(StoreError::Corrupt(CORRUPT_PAGE));
        }
    }
    let (off, n) = tail_span(published, file_len);
    if n > 0
        && off >= PAGE as u64
        && !flags.tail_restored
        && !(flags.no_slot && zero_tail_is_record(name))
    {
        let mut prefix = vec![0u8; n as usize];
        view.read(off, &mut prefix)?;
        if all_zero(&prefix) {
            return Err(StoreError::Corrupt(CORRUPT_PAGE));
        }
    }
    Ok(())
}
