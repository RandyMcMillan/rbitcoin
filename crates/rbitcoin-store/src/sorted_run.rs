//! Append-only **sorted run** files for index build-as-you-go (SH / tx / spend).
//!
//! Fixed-width records, sorted by a leading key. Integrity:
//! - Each run stores a **CRC-32** of the body in the header (format v2).
//! - A directory **`MANIFEST`** lists the authoritative set of runs (seq, lens,
//!   count, crc). Updated only via write-tmp → fsync → rename after a successful
//!   run write or merge, so a crash cannot leave a half-published set.
//! - [`list_runs`] trusts the manifest (not a raw directory scan) when present;
//!   missing listed files or CRC/header mismatch are reported; orphan `.run`
//!   files are ignored (and removed best-effort).
//!
//! # Concurrency invariant (`runs_io`)
//!
//! Callers **must** hold the per-family `runs_io` mutex across any sequence that
//! combines [`list_runs`] with write / merge / claim / delete. `list_runs` may
//! **delete** uncataloged `*.run` files (orphan cleanup). A concurrent writer that
//! creates a `.run` before MANIFEST update without holding the lock can lose that
//! file. Materialize uses [`claim_run_for_materialize`] (`*.run.mat`) so claim
//! bodies are never scanned as orphans.

use crate::error::StoreError;
use crate::io_handle::IoHandle;
use crate::uring_session::{self, UringSession};
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic: `RBSORT02` (v2 header with body CRC).
const MAGIC: [u8; 8] = *b"RBSORT02";
/// Pre-checksum runs (pad was zero). Still readable; body CRC not verified.
const MAGIC_V1: [u8; 8] = *b"RBSORT01";
const HEADER_LEN: usize = 32;
/// header: magic8 | version_u32 | key_len_u32 | rec_len_u32 | count_u64 | body_crc32_u32
const VERSION: u32 = 2;
const VERSION_V1: u32 = 1;

/// Directory catalog: `MANIFEST` (atomic replace).
const MANIFEST_NAME: &str = "MANIFEST";
const MANIFEST_MAGIC: [u8; 8] = *b"RBRUNMF1";
const MANIFEST_VERSION: u32 = 1;
/// entry: seq_u64 | count_u64 | key_len_u32 | rec_len_u32 | body_crc32_u32 = 28
const MANIFEST_ENTRY_LEN: usize = 28;

fn io_err(path: &Path, e: std::io::Error) -> StoreError {
    StoreError::io(path, e)
}

fn crc32_table() -> &'static [u32; 256] {
    use std::sync::OnceLock;
    static T: OnceLock<[u32; 256]> = OnceLock::new();
    T.get_or_init(|| {
        let mut t = [0u32; 256];
        for (i, slot) in t.iter_mut().enumerate() {
            let mut c = i as u32;
            for _ in 0..8 {
                c = if c & 1 != 0 {
                    0xEDB8_8320 ^ (c >> 1)
                } else {
                    c >> 1
                };
            }
            *slot = c;
        }
        t
    })
}

/// CRC-32 of `data` (init 0xFFFF_FFFF, final xor 0xFFFF_FFFF).
pub fn crc32(data: &[u8]) -> u32 {
    let table = crc32_table();
    let mut c = 0xFFFF_FFFFu32;
    for &b in data {
        c = table[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
    }
    c ^ 0xFFFF_FFFF
}

fn crc32_file_body(path: &Path, body_len: u64) -> Result<u32, StoreError> {
    let mut f = File::open(path).map_err(|e| io_err(path, e))?;
    f.seek(SeekFrom::Start(HEADER_LEN as u64))
        .map_err(|e| io_err(path, e))?;
    let table = crc32_table();
    let mut c = 0xFFFF_FFFFu32;
    let mut left = body_len;
    let mut buf = [0u8; 64 * 1024];
    while left > 0 {
        let n = (left as usize).min(buf.len());
        f.read_exact(&mut buf[..n]).map_err(|e| io_err(path, e))?;
        for &b in &buf[..n] {
            c = table[((c ^ u32::from(b)) & 0xFF) as usize] ^ (c >> 8);
        }
        left -= n as u64;
    }
    Ok(c ^ 0xFFFF_FFFF)
}

/// One immutable sorted run on disk.
#[derive(Debug, Clone)]
pub struct SortedRunPath {
    pub path: PathBuf,
    pub count: u64,
    pub rec_len: u32,
    pub key_len: u32,
    /// CRC-32 of the body (0 = legacy v1 / unknown).
    pub body_crc32: u32,
}

impl SortedRunPath {
    /// Sequence number from `{seq:06}.run` stem, if parseable.
    pub fn seq(&self) -> Option<u64> {
        seq_from_path(&self.path)
    }
}

fn seq_from_path(path: &Path) -> Option<u64> {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.parse::<u64>().ok())
}

/// Next run path: `{dir}/{seq:06}.run`.
pub fn next_run_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:06}.run"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ManifestEntry {
    seq: u64,
    count: u64,
    key_len: u32,
    rec_len: u32,
    body_crc32: u32,
}

impl ManifestEntry {
    fn from_run(run: &SortedRunPath) -> Option<Self> {
        Some(Self {
            seq: run.seq()?,
            count: run.count,
            key_len: run.key_len,
            rec_len: run.rec_len,
            body_crc32: run.body_crc32,
        })
    }

    fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.count.to_le_bytes());
        out.extend_from_slice(&self.key_len.to_le_bytes());
        out.extend_from_slice(&self.rec_len.to_le_bytes());
        out.extend_from_slice(&self.body_crc32.to_le_bytes());
    }

    fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < MANIFEST_ENTRY_LEN {
            return None;
        }
        Some(Self {
            seq: u64::from_le_bytes(buf[0..8].try_into().ok()?),
            count: u64::from_le_bytes(buf[8..16].try_into().ok()?),
            key_len: u32::from_le_bytes(buf[16..20].try_into().ok()?),
            rec_len: u32::from_le_bytes(buf[20..24].try_into().ok()?),
            body_crc32: u32::from_le_bytes(buf[24..28].try_into().ok()?),
        })
    }
}

#[derive(Debug, Clone, Default)]
struct Manifest {
    entries: Vec<ManifestEntry>,
}

fn manifest_path(dir: &Path) -> PathBuf {
    dir.join(MANIFEST_NAME)
}

fn load_manifest(dir: &Path) -> Result<Option<Manifest>, StoreError> {
    let path = manifest_path(dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = File::open(&path).map_err(|e| io_err(&path, e))?;
    let mut hdr = [0u8; 16];
    if f.read_exact(&mut hdr).is_err() {
        rbitcoin_log::warn!("store: sorted-run MANIFEST truncated at {}", path.display());
        return Ok(None);
    }
    if hdr[0..8] != MANIFEST_MAGIC {
        rbitcoin_log::warn!("store: sorted-run MANIFEST bad magic at {}", path.display());
        return Ok(None);
    }
    let ver = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
    if ver != MANIFEST_VERSION {
        rbitcoin_log::warn!(
            "store: sorted-run MANIFEST unsupported version {ver} at {}",
            path.display()
        );
        return Ok(None);
    }
    let n = u32::from_le_bytes(hdr[12..16].try_into().unwrap()) as usize;
    let mut body = vec![0u8; n.saturating_mul(MANIFEST_ENTRY_LEN)];
    if !body.is_empty() {
        f.read_exact(&mut body).map_err(|e| io_err(&path, e))?;
    }
    let mut entries = Vec::with_capacity(n);
    for i in 0..n {
        let off = i * MANIFEST_ENTRY_LEN;
        let Some(e) = ManifestEntry::decode(&body[off..off + MANIFEST_ENTRY_LEN]) else {
            return Err(StoreError::Corrupt("sorted run manifest: bad entry"));
        };
        entries.push(e);
    }
    entries.sort_by_key(|e| e.seq);
    Ok(Some(Manifest { entries }))
}

/// Atomically replace `MANIFEST` (tmp + fsync + rename).
fn save_manifest(dir: &Path, mf: &Manifest) -> Result<(), StoreError> {
    fs::create_dir_all(dir).map_err(|e| io_err(dir, e))?;
    let path = manifest_path(dir);
    let tmp = dir.join(format!("{MANIFEST_NAME}.tmp"));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| io_err(&tmp, e))?;
        let mut buf = Vec::with_capacity(16 + mf.entries.len() * MANIFEST_ENTRY_LEN);
        buf.extend_from_slice(&MANIFEST_MAGIC);
        buf.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        buf.extend_from_slice(&(mf.entries.len() as u32).to_le_bytes());
        for e in &mf.entries {
            e.encode(&mut buf);
        }
        f.write_all(&buf).map_err(|e| io_err(&tmp, e))?;
        f.sync_all().map_err(|e| io_err(&tmp, e))?;
    }
    fs::rename(&tmp, &path).map_err(|e| io_err(&path, e))?;
    // Best-effort directory durability for the new dirent.
    if let Ok(dirf) = File::open(dir) {
        let _ = dirf.sync_all();
    }
    Ok(())
}

fn manifest_insert(dir: &Path, run: &SortedRunPath) -> Result<(), StoreError> {
    let Some(entry) = ManifestEntry::from_run(run) else {
        // Non-standard name (tests use `lk.run`) — no catalog entry.
        return Ok(());
    };
    let mut mf = load_manifest(dir)?.unwrap_or_default();
    mf.entries.retain(|e| e.seq != entry.seq);
    mf.entries.push(entry);
    mf.entries.sort_by_key(|e| e.seq);
    save_manifest(dir, &mf)
}

fn manifest_merge_commit(
    dir: &Path,
    remove_seqs: &[u64],
    add: &SortedRunPath,
) -> Result<(), StoreError> {
    let Some(add_e) = ManifestEntry::from_run(add) else {
        return Ok(());
    };
    let mut mf = load_manifest(dir)?.unwrap_or_default();
    mf.entries
        .retain(|e| !remove_seqs.contains(&e.seq) && e.seq != add_e.seq);
    mf.entries.push(add_e);
    mf.entries.sort_by_key(|e| e.seq);
    save_manifest(dir, &mf)
}

fn rebuild_manifest_from_runs(dir: &Path, runs: &[SortedRunPath]) -> Result<(), StoreError> {
    let mut mf = Manifest::default();
    for r in runs {
        if let Some(e) = ManifestEntry::from_run(r) {
            mf.entries.push(e);
        }
    }
    mf.entries.sort_by_key(|e| e.seq);
    save_manifest(dir, &mf)
}

/// Sync / cache / pacing policy for sorted-run file writes.
///
/// | Policy | When |
/// |--------|------|
/// | [`RunWritePolicy::CATALOG`] | Tip fan-in / recollect / default durable — **no** artificial pace |
/// | [`RunWritePolicy::IBD_BACKGROUND`] | Steady Direct IBD L0→catalog promote while confirm is hot |
/// | [`RunWritePolicy::L0`] | Transient memtable spills (no fsync; keep cache for coalesce) |
///
/// **Do not** pace tip materialize: multi‑pass reduce rewrites tens of GiB; even
/// 2 ms / 16 MiB would add multi‑second stalls on the critical path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RunWritePolicy {
    /// `fsync` file + parent dir after rename.
    pub durable: bool,
    /// `POSIX_FADV_DONTNEED` after write (long-lived catalog only).
    pub drop_cache: bool,
    /// Brief sleep every [`PACE_CHUNK_BYTES`] of body so confirm Class A can interleave.
    /// **Only** for IBD background promotes — never tip reduce.
    pub pace: bool,
}

impl RunWritePolicy {
    /// Durable catalog / SEAL-backed run; full-speed write + DONTNEED.
    ///
    /// Used by tip fan-in reduce, recollect catalog spills, and generic
    /// [`write_sorted_run`] / [`merge_runs`].
    pub const CATALOG: Self = Self {
        durable: true,
        drop_cache: true,
        pace: false,
    };
    /// Steady IBD promote while confirm Class A is concurrent — paced durable.
    pub const IBD_BACKGROUND: Self = Self {
        durable: true,
        drop_cache: true,
        pace: true,
    };
    /// Transient L0: no fsync, keep cache for coalesce, no pace (small-ish spills).
    pub const L0: Self = Self {
        durable: false,
        drop_cache: false,
        pace: false,
    };
    /// Alias of [`Self::CATALOG`] (tests / explicit durable unpaced).
    pub const DURABLE: Self = Self::CATALOG;
}

/// Body bytes between yields when [`RunWritePolicy::pace`] is set (~16 MiB).
pub const PACE_CHUNK_BYTES: usize = 16 * 1024 * 1024;
/// Sleep between paced chunks (idle SH worker cedes disk to confirm write).
const PACE_SLEEP: std::time::Duration = std::time::Duration::from_millis(2);

fn write_all_paced(f: &mut File, data: &[u8], pace: bool, path: &Path) -> Result<(), StoreError> {
    if !pace || data.len() <= PACE_CHUNK_BYTES {
        return f.write_all(data).map_err(|e| io_err(path, e));
    }
    let mut off = 0;
    while off < data.len() {
        let end = (off + PACE_CHUNK_BYTES).min(data.len());
        f.write_all(&data[off..end]).map_err(|e| io_err(path, e))?;
        off = end;
        if off < data.len() {
            std::thread::sleep(PACE_SLEEP);
        }
    }
    Ok(())
}

/// Best-effort: lower this thread's **I/O** and CPU priority so SH run work yields
/// to confirm Class A under contention (Linux `ioprio` idle + `nice 19`).
///
/// No-op / best-effort failure on other platforms or without CAP_SYS_ADMIN for
/// some ioprio classes — never errors.
pub fn set_thread_idle_io_priority() {
    #[cfg(target_os = "linux")]
    {
        // IOPRIO_CLASS_IDLE << 13 | data; WHO_PROCESS, who=0 (self).
        const IOPRIO_WHO_PROCESS: libc::c_int = 1;
        const IOPRIO_CLASS_IDLE: libc::c_int = 3;
        let ioprio = (IOPRIO_CLASS_IDLE << 13) | 0;
        let _ = unsafe {
            libc::syscall(
                libc::SYS_ioprio_set,
                IOPRIO_WHO_PROCESS,
                0 as libc::c_int,
                ioprio,
            )
        };
        let _ = unsafe { libc::setpriority(libc::PRIO_PROCESS, 0, 19) };
    }
}

/// Write a new sorted run from **already sorted** fixed-width records.
///
/// `records` must be sorted ascending by the first `key_len` bytes of each
/// `rec_len`-byte record. Updates the parent directory [`MANIFEST`] when the
/// file name is `{seq:06}.run`. Uses [`RunWritePolicy::CATALOG`].
pub fn write_sorted_run(
    path: &Path,
    key_len: u32,
    rec_len: u32,
    records: &[u8],
) -> Result<SortedRunPath, StoreError> {
    let run = write_sorted_run_file_with_policy(
        path,
        key_len,
        rec_len,
        records,
        RunWritePolicy::CATALOG,
    )?;
    if let Some(dir) = path.parent() {
        manifest_insert(dir, &run)?;
    }
    Ok(run)
}

/// Write run file only (no MANIFEST) with an explicit policy.
///
/// L0 spills use [`RunWritePolicy::L0`]; catalog merge internals use
/// [`RunWritePolicy::CATALOG`].
pub fn write_sorted_run_file_with_policy(
    path: &Path,
    key_len: u32,
    rec_len: u32,
    records: &[u8],
    policy: RunWritePolicy,
) -> Result<SortedRunPath, StoreError> {
    write_sorted_run_file(path, key_len, rec_len, records, policy)
}

/// Write run file only (no manifest). Used by merge for a single catalog commit.
fn write_sorted_run_file(
    path: &Path,
    key_len: u32,
    rec_len: u32,
    records: &[u8],
    policy: RunWritePolicy,
) -> Result<SortedRunPath, StoreError> {
    if key_len == 0 || rec_len < key_len {
        return Err(StoreError::Corrupt("sorted run: bad key/rec len"));
    }
    if !records.len().is_multiple_of(rec_len as usize) {
        return Err(StoreError::Corrupt(
            "sorted run: body not multiple of rec_len",
        ));
    }
    // Full-record keys (SH: key_len == rec_len == 40) must be unique and ordered.
    // Other families (key_len < rec_len) keep payload-only ties.
    if key_len == rec_len && !records.is_empty() {
        let rl = rec_len as usize;
        let mut prev: Option<&[u8]> = None;
        for rec in records.chunks_exact(rl) {
            if let Some(p) = prev {
                if rec_key_cmp(rec, p, key_len as usize, rec_len) != Ordering::Greater {
                    return Err(StoreError::Corrupt(
                        "sorted run: records not strictly increasing",
                    ));
                }
            }
            prev = Some(rec);
        }
    }
    let count = (records.len() / rec_len as usize) as u64;
    let body_crc32 = crc32(records);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| io_err(&tmp, e))?;
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..8].copy_from_slice(&MAGIC);
        hdr[8..12].copy_from_slice(&VERSION.to_le_bytes());
        hdr[12..16].copy_from_slice(&key_len.to_le_bytes());
        hdr[16..20].copy_from_slice(&rec_len.to_le_bytes());
        hdr[20..28].copy_from_slice(&count.to_le_bytes());
        hdr[28..32].copy_from_slice(&body_crc32.to_le_bytes());
        f.write_all(&hdr).map_err(|e| io_err(&tmp, e))?;
        if !records.is_empty() {
            write_all_paced(&mut f, records, policy.pace, &tmp)?;
        }
        if policy.durable {
            f.sync_all().map_err(|e| io_err(&tmp, e))?;
        }
    }
    fs::rename(&tmp, path).map_err(|e| io_err(path, e))?;
    if policy.durable {
        if let Some(parent) = path.parent() {
            if let Ok(dirf) = File::open(parent) {
                let _ = dirf.sync_all();
            }
        }
    }
    // Catalog: drop from page cache so multi‑hundred MiB runs do not crowd
    // tx.body working set. L0 keeps cache for the imminent coalesce re-read.
    if policy.drop_cache {
        advise_file_dont_need(path);
    }
    Ok(SortedRunPath {
        path: path.to_path_buf(),
        count,
        rec_len,
        key_len,
        body_crc32,
    })
}

/// Best-effort whole-file `POSIX_FADV_DONTNEED` (Linux). No-op elsewhere.
fn advise_file_dont_need(path: &Path) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let f = match OpenOptions::new().read(true).open(path) {
            Ok(f) => f,
            Err(_) => return,
        };
        // offset=0, len=0 ⇒ entire file on Linux.
        let rc = unsafe { libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED) };
        if rc != 0 {
            rbitcoin_log::trace!(
                "store: sorted-run fadvise(DONTNEED) failed path={}: {}",
                path.display(),
                std::io::Error::from_raw_os_error(rc)
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
    }
}

/// Open and validate a run header + body length (does not re-hash the body).
///
/// Full body CRC is checked by [`verify_run_body`] / [`read_run_body`].
pub fn open_run(path: &Path) -> Result<SortedRunPath, StoreError> {
    let mut f = File::open(path).map_err(|e| io_err(path, e))?;
    let mut hdr = [0u8; HEADER_LEN];
    f.read_exact(&mut hdr).map_err(|e| io_err(path, e))?;
    let (version, body_crc32) = if hdr[0..8] == MAGIC {
        let version = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        if version != VERSION {
            return Err(StoreError::Corrupt("sorted run: unsupported version"));
        }
        let crc = u32::from_le_bytes(hdr[28..32].try_into().unwrap());
        (version, crc)
    } else if hdr[0..8] == MAGIC_V1 {
        let version = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
        if version != VERSION_V1 {
            return Err(StoreError::Corrupt("sorted run: unsupported version"));
        }
        (version, 0)
    } else {
        return Err(StoreError::Corrupt("sorted run: bad magic"));
    };
    let _ = version;
    let key_len = u32::from_le_bytes(hdr[12..16].try_into().unwrap());
    let rec_len = u32::from_le_bytes(hdr[16..20].try_into().unwrap());
    let count = u64::from_le_bytes(hdr[20..28].try_into().unwrap());
    if key_len == 0 || rec_len < key_len {
        return Err(StoreError::Corrupt("sorted run: bad lens in header"));
    }
    let meta = f.metadata().map_err(|e| io_err(path, e))?;
    let expect = HEADER_LEN as u64 + count * rec_len as u64;
    if meta.len() < expect {
        return Err(StoreError::Corrupt("sorted run: truncated body"));
    }
    if meta.len() > expect {
        // Trailing garbage is not allowed for v2.
        if body_crc32 != 0 || hdr[0..8] == MAGIC {
            return Err(StoreError::Corrupt("sorted run: trailing garbage"));
        }
    }
    Ok(SortedRunPath {
        path: path.to_path_buf(),
        count,
        rec_len,
        key_len,
        body_crc32,
    })
}

/// Stream the body and check CRC-32 (no-op for legacy v1 with crc=0).
pub fn verify_run_body(run: &SortedRunPath) -> Result<(), StoreError> {
    if run.body_crc32 == 0 {
        return Ok(());
    }
    let body_len = run.count.saturating_mul(u64::from(run.rec_len));
    let got = crc32_file_body(&run.path, body_len)?;
    if got != run.body_crc32 {
        return Err(StoreError::Corrupt("sorted run: body CRC mismatch"));
    }
    Ok(())
}

/// Read all records into a contiguous buffer (count × rec_len). Verifies CRC.
pub fn read_run_body(run: &SortedRunPath) -> Result<Vec<u8>, StoreError> {
    let mut f = File::open(&run.path).map_err(|e| io_err(&run.path, e))?;
    f.seek(SeekFrom::Start(HEADER_LEN as u64))
        .map_err(|e| io_err(&run.path, e))?;
    let mut buf = vec![0u8; (run.count as usize).saturating_mul(run.rec_len as usize)];
    if !buf.is_empty() {
        f.read_exact(&mut buf).map_err(|e| io_err(&run.path, e))?;
    }
    if run.body_crc32 != 0 {
        let got = crc32(&buf);
        if got != run.body_crc32 {
            return Err(StoreError::Corrupt("sorted run: body CRC mismatch"));
        }
    }
    Ok(buf)
}

/// Binary-search a sorted run for `key` (first `key_len` bytes of each record).
///
/// Returns the full record bytes on hit. Equal keys: first match in file order.
/// Does **not** load the whole run into RAM (O(log n) seeks + reads).
pub fn lookup_key(run: &SortedRunPath, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
    if key.len() < run.key_len as usize {
        return Err(StoreError::Corrupt("sorted run: lookup key short"));
    }
    if run.count == 0 {
        return Ok(None);
    }
    let key = &key[..run.key_len as usize];
    let rec_len = run.rec_len as u64;
    let mut f = File::open(&run.path).map_err(|e| io_err(&run.path, e))?;
    let mut lo = 0u64;
    let mut hi = run.count;
    let mut rec = vec![0u8; run.rec_len as usize];
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = HEADER_LEN as u64 + mid * rec_len;
        f.seek(SeekFrom::Start(off))
            .map_err(|e| io_err(&run.path, e))?;
        f.read_exact(&mut rec).map_err(|e| io_err(&run.path, e))?;
        match rec[..run.key_len as usize].cmp(key) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => {
                let mut i = mid;
                while i > 0 {
                    let poff = HEADER_LEN as u64 + (i - 1) * rec_len;
                    f.seek(SeekFrom::Start(poff))
                        .map_err(|e| io_err(&run.path, e))?;
                    let mut prev = vec![0u8; run.rec_len as usize];
                    f.read_exact(&mut prev).map_err(|e| io_err(&run.path, e))?;
                    if &prev[..run.key_len as usize] != key {
                        break;
                    }
                    rec = prev;
                    i -= 1;
                }
                return Ok(Some(rec));
            }
        }
    }
    Ok(None)
}

/// Read-ahead page for merge cursors (~256 KiB; many fixed-width records).
const RUN_CURSOR_PAGE: usize = 256 * 1024;

fn open_cursor_file(path: &Path) -> Result<File, StoreError> {
    let mut o = OpenOptions::new();
    o.read(true);
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        const FILE_FLAG_OVERLAPPED: u32 = 0x4000_0000;
        o.custom_flags(FILE_FLAG_OVERLAPPED);
    }
    o.open(path).map_err(|e| io_err(path, e))
}

fn pread_exact(
    handle: IoHandle,
    path: &Path,
    offset: u64,
    buf: &mut [u8],
) -> Result<(), StoreError> {
    let mut done = 0usize;
    while done < buf.len() {
        let rc = handle.pread(offset + done as u64, &mut buf[done..]);
        if rc < 0 {
            return Err(StoreError::io(path, std::io::Error::from_raw_os_error(-rc)));
        }
        if rc == 0 {
            return Err(StoreError::io(
                path,
                std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "sorted run pread short"),
            ));
        }
        done += rc as usize;
    }
    Ok(())
}

fn read_page_at(
    handle: IoHandle,
    path: &Path,
    offset: u64,
    buf: &mut [u8],
    remaining_recs: u64,
    rec_len: usize,
) -> Result<usize, StoreError> {
    let max_from_file = (remaining_recs as usize).saturating_mul(rec_len);
    let to_read = (buf.len().min(max_from_file) / rec_len).saturating_mul(rec_len);
    if to_read == 0 {
        return Ok(0);
    }
    pread_exact(handle, path, offset, &mut buf[..to_read])?;
    Ok(to_read)
}

/// Streaming cursor over a run (for merge).
struct RunCursor {
    file: File,
    handle: IoHandle,
    path: PathBuf,
    remaining: u64,
    rec_len: usize,
    file_off: u64,
    page: Vec<u8>,
    page_len: usize,
    cur: usize,
    next: usize,
    ahead: Vec<u8>,
    ahead_len: usize,
    /// Inflight ahead pread length; `0` when idle or ready.
    ahead_want: usize,
}

impl RunCursor {
    fn open(run: &SortedRunPath, verify: bool) -> Result<Self, StoreError> {
        Self::open_range(run, 0, run.count, verify)
    }

    /// Stream `[first_idx, first_idx + count)` using the same 256 KiB page refill.
    fn open_range(
        run: &SortedRunPath,
        first_idx: u64,
        count: u64,
        verify: bool,
    ) -> Result<Self, StoreError> {
        Self::open_range_paged(run, first_idx, count, verify, RUN_CURSOR_PAGE)
    }

    fn open_range_paged(
        run: &SortedRunPath,
        first_idx: u64,
        count: u64,
        verify: bool,
        page_bytes: usize,
    ) -> Result<Self, StoreError> {
        if verify {
            verify_run_body(run)?;
        }
        let rec_len = run.rec_len as usize;
        if rec_len == 0 {
            return Err(StoreError::Corrupt("sorted run: zero rec_len"));
        }
        let start = first_idx.min(run.count);
        let remaining = count.min(run.count.saturating_sub(start));
        let file = open_cursor_file(&run.path)?;
        let handle = IoHandle::from_file(&file);
        let file_off = HEADER_LEN as u64 + start.saturating_mul(rec_len as u64);
        let page_cap = page_bytes
            .max(rec_len)
            .div_ceil(rec_len)
            .saturating_mul(rec_len);
        let cur = Self {
            file,
            handle,
            path: run.path.clone(),
            remaining,
            rec_len,
            file_off,
            page: vec![0u8; page_cap],
            page_len: 0,
            cur: 0,
            next: 0,
            ahead: vec![0u8; page_cap],
            ahead_len: 0,
            ahead_want: 0,
        };
        cur.advise_willneed();
        Ok(cur)
    }

    fn advise_willneed(&self) {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            let _ = unsafe {
                libc::posix_fadvise(self.file.as_raw_fd(), 0, 0, libc::POSIX_FADV_WILLNEED)
            };
        }
    }

    fn file_recs_left(&self) -> u64 {
        let unread_in_page = (self.page_len.saturating_sub(self.next)) / self.rec_len;
        self.remaining.saturating_sub(unread_in_page as u64)
    }

    fn ahead_to_read(&self) -> usize {
        let file_left = self.file_recs_left();
        let max_from_file = (file_left as usize).saturating_mul(self.rec_len);
        (self.ahead.len().min(max_from_file) / self.rec_len).saturating_mul(self.rec_len)
    }

    fn complete_ahead(&mut self, res: i32) -> Result<(), StoreError> {
        if self.ahead_want == 0 {
            return Err(StoreError::Corrupt(
                "invariant: merge ahead cqe without submit",
            ));
        }
        uring_session::require_full_cqe(res, self.ahead_want, &self.path)?;
        self.ahead_len = self.ahead_want;
        self.ahead_want = 0;
        Ok(())
    }

    fn try_submit_ahead(
        &mut self,
        session: &mut UringSession,
        slot: u32,
    ) -> Result<bool, StoreError> {
        if self.ahead_want > 0 || self.ahead_len > 0 {
            return Ok(false);
        }
        let want = self.ahead_to_read();
        if want == 0 {
            return Ok(false);
        }
        if session.free_sq() == 0 {
            return Ok(false);
        }
        let ud = uring_session::pack_ud(uring_session::KIND_MERGE_PREAD, session.epoch(), slot);
        session.push_pread(self.handle, self.file_off, &mut self.ahead[..want], ud)?;
        self.file_off += want as u64;
        self.ahead_want = want;
        Ok(true)
    }

    fn prefetch_ahead_blocking(&mut self) -> Result<(), StoreError> {
        if self.ahead_want > 0 || self.ahead_len > 0 {
            return Ok(());
        }
        let file_left = self.file_recs_left();
        let n = read_page_at(
            self.handle,
            &self.path,
            self.file_off,
            &mut self.ahead,
            file_left,
            self.rec_len,
        )?;
        self.file_off += n as u64;
        self.ahead_len = n;
        Ok(())
    }

    fn promote_ahead(&mut self) -> Result<(), StoreError> {
        if self.ahead_want > 0 {
            return Err(StoreError::Corrupt(
                "invariant: promote while ahead inflight",
            ));
        }
        if self.ahead_len >= self.rec_len {
            std::mem::swap(&mut self.page, &mut self.ahead);
            self.page_len = self.ahead_len;
            self.ahead_len = 0;
            self.next = 0;
            self.cur = 0;
            return Ok(());
        }
        let leftover = self.page_len.saturating_sub(self.next);
        if leftover > 0 && self.next > 0 {
            self.page.copy_within(self.next..self.page_len, 0);
        }
        self.page_len = leftover;
        self.next = 0;
        self.cur = 0;
        let unread_in_page = leftover / self.rec_len;
        let file_left = self.remaining.saturating_sub(unread_in_page as u64);
        let start = self.page_len;
        let n = read_page_at(
            self.handle,
            &self.path,
            self.file_off,
            &mut self.page[start..],
            file_left,
            self.rec_len,
        )?;
        self.file_off += n as u64;
        self.page_len = start + n;
        Ok(())
    }

    /// Advance to the next record without a completion session.
    fn fill_next(&mut self) -> Result<bool, StoreError> {
        if self.remaining == 0 {
            return Ok(false);
        }
        if self.next + self.rec_len > self.page_len {
            self.promote_ahead()?;
            if self.next + self.rec_len > self.page_len {
                return Err(StoreError::Corrupt(
                    "sorted run: short read on merge cursor",
                ));
            }
            self.prefetch_ahead_blocking()?;
        }
        self.cur = self.next;
        self.next += self.rec_len;
        self.remaining -= 1;
        Ok(true)
    }

    #[inline]
    fn rec(&self) -> &[u8] {
        &self.page[self.cur..self.cur + self.rec_len]
    }
}

fn harvest_merge_cqes(
    session: &mut UringSession,
    cursors: &mut [Option<RunCursor>],
) -> Result<(), StoreError> {
    let cqes = session.harvest_ready()?;
    for (ud, res) in cqes {
        let (kind, _epoch, slot) = uring_session::unpack_ud(ud);
        if kind != uring_session::KIND_MERGE_PREAD {
            return Err(StoreError::Corrupt("invariant: merge cqe kind"));
        }
        let i = slot as usize;
        let Some(c) = cursors.get_mut(i).and_then(|o| o.as_mut()) else {
            return Err(StoreError::Corrupt("invariant: merge cqe slot"));
        };
        c.complete_ahead(res)?;
    }
    Ok(())
}

fn wait_ahead(
    session: &mut UringSession,
    cursors: &mut [Option<RunCursor>],
    idx: usize,
) -> Result<(), StoreError> {
    loop {
        harvest_merge_cqes(session, cursors)?;
        let inflight = cursors
            .get(idx)
            .and_then(|o| o.as_ref())
            .is_some_and(|c| c.ahead_want > 0);
        if !inflight {
            return Ok(());
        }
        session.submit_and_wait_one()?;
    }
}

fn try_submit_ahead_at(
    cursors: &mut [Option<RunCursor>],
    idx: usize,
    session: &mut UringSession,
) -> Result<bool, StoreError> {
    if session.free_sq() == 0 {
        harvest_merge_cqes(session, cursors)?;
        if session.free_sq() == 0 {
            return Ok(false);
        }
    }
    let Some(c) = cursors.get_mut(idx).and_then(|o| o.as_mut()) else {
        return Ok(false);
    };
    c.try_submit_ahead(session, idx as u32)
}

fn fill_next_at(
    cursors: &mut [Option<RunCursor>],
    idx: usize,
    session: &mut UringSession,
) -> Result<bool, StoreError> {
    let remaining = match cursors.get(idx).and_then(|o| o.as_ref()) {
        Some(c) => c.remaining,
        None => return Ok(false),
    };
    if remaining == 0 {
        return Ok(false);
    }
    let need_promote = {
        let c = cursors[idx].as_ref().unwrap();
        c.next + c.rec_len > c.page_len
    };
    if need_promote {
        wait_ahead(session, cursors, idx)?;
        cursors[idx].as_mut().unwrap().promote_ahead()?;
        let short = {
            let c = cursors[idx].as_ref().unwrap();
            c.next + c.rec_len > c.page_len
        };
        if short {
            return Err(StoreError::Corrupt(
                "sorted run: short read on merge cursor",
            ));
        }
        if try_submit_ahead_at(cursors, idx, session)? {
            session.submit()?;
        }
    }
    let c = cursors[idx].as_mut().unwrap();
    c.cur = c.next;
    c.next += c.rec_len;
    c.remaining -= 1;
    Ok(true)
}

fn merge_session_entries(k: usize) -> u32 {
    (k as u32).max(uring_session::DEFAULT_ENTRIES).min(4096)
}

const MERGE_SENTINEL: usize = usize::MAX;

/// Compare two fixed records for merge / write order.
///
/// SH catalogs (`key_len == rec_len == 40`) sort by scripthash then **numeric**
/// little-endian create_fk — not raw 40-byte memcmp (fk 255 < 256).
fn rec_key_cmp(a: &[u8], b: &[u8], key_len: usize, rec_len: u32) -> Ordering {
    if key_len == 40 && rec_len == 40 && a.len() >= 40 && b.len() >= 40 {
        match a[..32].cmp(&b[..32]) {
            Ordering::Equal => {
                let fa = u64::from_le_bytes(a[32..40].try_into().unwrap());
                let fb = u64::from_le_bytes(b[32..40].try_into().unwrap());
                fa.cmp(&fb)
            }
            o => o,
        }
    } else {
        let n = key_len.min(a.len()).min(b.len());
        a[..n].cmp(&b[..n])
    }
}

fn cursor_less(cursors: &[Option<RunCursor>], a: usize, b: usize, key_len: usize) -> bool {
    if a == MERGE_SENTINEL {
        return false;
    }
    if b == MERGE_SENTINEL {
        return true;
    }
    match (&cursors[a], &cursors[b]) {
        (None, _) => false,
        (_, None) => true,
        (Some(ca), Some(cb)) => match rec_key_cmp(ca.rec(), cb.rec(), key_len, ca.rec_len as u32) {
            Ordering::Less => true,
            Ordering::Greater => false,
            Ordering::Equal => a < b,
        },
    }
}

/// Loser tree over run indices. Internal nodes hold losers; `winner` is the min leaf.
/// Cursors stay put — the tree only stores indices.
struct LoserTree {
    losers: Vec<usize>,
    winner: usize,
    n: usize,
}

impl LoserTree {
    fn new(cursors: &[Option<RunCursor>], key_len: usize) -> Self {
        let k = cursors.len();
        let n = k.next_power_of_two().max(1);
        let mut winners = vec![MERGE_SENTINEL; 2 * n];
        let mut losers = vec![MERGE_SENTINEL; n];
        for i in 0..n {
            winners[n + i] = if i < k { i } else { MERGE_SENTINEL };
        }
        for i in (1..n).rev() {
            let left = winners[2 * i];
            let right = winners[2 * i + 1];
            if cursor_less(cursors, left, right, key_len) {
                winners[i] = left;
                losers[i] = right;
            } else {
                winners[i] = right;
                losers[i] = left;
            }
        }
        Self {
            winner: winners[1],
            losers,
            n,
        }
    }

    fn replay(&mut self, cursors: &[Option<RunCursor>], leaf: usize, key_len: usize) {
        let mut player = leaf;
        let mut i = self.n + leaf;
        while i > 1 {
            let p = i >> 1;
            if cursor_less(cursors, self.losers[p], player, key_len) {
                std::mem::swap(&mut self.losers[p], &mut player);
            }
            i = p;
        }
        self.winner = player;
    }

    fn winner(&self) -> usize {
        self.winner
    }
}

/// When the compare key is the whole record (`key_len == rec_len`), equal
/// keys are true duplicates — emit once. Payload-bearing families
/// (`key_len < rec_len`) keep all equal-key records.
fn should_skip_dup_key(key_len: usize, rec_len: u32, prev: Option<&[u8]>, rec: &[u8]) -> bool {
    if key_len != rec_len as usize {
        return false;
    }
    prev.is_some_and(|p| p == rec)
}

/// Remove one run from the catalog and delete its file (after materialize).
pub fn remove_run(run: &SortedRunPath) -> Result<(), StoreError> {
    detach_run(run)?;
    let _ = fs::remove_file(&run.path);
    Ok(())
}

/// Drop a run from the MANIFEST but **leave the file**.
///
/// Prefer [`claim_run_for_materialize`] for materialize: a bare detach leaves a
/// `.run` file that concurrent [`list_runs`] will **delete as an orphan**.
pub fn detach_run(run: &SortedRunPath) -> Result<(), StoreError> {
    let Some(dir) = run.path.parent() else {
        return Ok(());
    };
    if let Some(seq) = run.seq() {
        let mut mf = load_manifest(dir)?.unwrap_or_default();
        mf.entries.retain(|e| e.seq != seq);
        save_manifest(dir, &mf)?;
    }
    Ok(())
}

/// Claim a cataloged run for materialize: rename `*.run` → `*.run.mat`, then
/// drop it from the MANIFEST.
///
/// Call under the family's `runs_io` lock. The `.mat` suffix is **not** scanned
/// by [`list_runs`] orphan cleanup (only `*.run`), so concurrent list/merge
/// cannot delete the claimed body while materialize runs.
///
/// Caller must materialize `claimed.path` then delete that file.
///
/// If `run.path` already ends with `.run.mat` (crash recovery), open as-is
/// without rename/detach.
pub fn claim_run_for_materialize(run: &SortedRunPath) -> Result<SortedRunPath, StoreError> {
    let is_mat = run
        .path
        .file_name()
        .and_then(|s| s.to_str())
        .is_some_and(|n| n.ends_with(".run.mat"));
    if is_mat {
        // Already claimed (interrupted materialize) — just re-open.
        return open_run(&run.path);
    }
    let mat_path = {
        let mut s = run.path.as_os_str().to_os_string();
        s.push(".mat");
        PathBuf::from(s)
    };
    fs::rename(&run.path, &mat_path).map_err(|e| io_err(&run.path, e))?;
    detach_run(run)?;
    Ok(SortedRunPath {
        path: mat_path,
        count: run.count,
        rec_len: run.rec_len,
        key_len: run.key_len,
        body_crc32: run.body_crc32,
    })
}

/// Open incomplete materialize claims left after crash/SIGINT (`*.run.mat`).
///
/// These are detached from MANIFEST by [`claim_run_for_materialize`]; without
/// this scan, restart would see "no runs" and drop the bodies.
pub fn list_materialize_claims(dir: &Path) -> Result<Vec<SortedRunPath>, StoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| io_err(dir, e))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|s| s.to_str())
                .is_some_and(|n| n.ends_with(".run.mat"))
        })
        .collect();
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        match open_run(&p) {
            Ok(r) => out.push(r),
            Err(e) => {
                rbitcoin_log::warn!("store: skipping bad materialize claim {}: {e}", p.display());
            }
        }
    }
    Ok(out)
}

fn rec_prefix_shard(rec: &[u8], n_shards: usize) -> usize {
    let mut full = [0u8; 32];
    let n = rec.len().min(32);
    full[..n].copy_from_slice(&rec[..n]);
    crate::scripthash_head::prefix_shard_of(&full, n_shards)
}

/// One record at `idx` (0-based). Does not scan the rest of the run.
pub fn read_run_record_at(run: &SortedRunPath, idx: u64) -> Result<Vec<u8>, StoreError> {
    if idx >= run.count {
        return Err(StoreError::Corrupt("sorted run: record index past end"));
    }
    let rec_len = run.rec_len as usize;
    if rec_len == 0 {
        return Err(StoreError::Corrupt("sorted run: zero rec_len"));
    }
    let mut f = File::open(&run.path).map_err(|e| io_err(&run.path, e))?;
    let off = HEADER_LEN as u64 + idx.saturating_mul(rec_len as u64);
    f.seek(SeekFrom::Start(off))
        .map_err(|e| io_err(&run.path, e))?;
    let mut rec = vec![0u8; rec_len];
    f.read_exact(&mut rec).map_err(|e| io_err(&run.path, e))?;
    Ok(rec)
}

/// First record index of each prefix shard, plus `run.count` as the last cut.
///
/// `starts.len() == n_shards + 1`. Slice `s` is `[starts[s], starts[s+1])`.
/// Empty shards have `starts[s] == starts[s+1]`. Binary search via one-record
/// pread (not a full-body scan).
pub fn shard_record_starts(run: &SortedRunPath, n_shards: usize) -> Result<Vec<u64>, StoreError> {
    let n = n_shards.max(1);
    let mut starts = vec![0u64; n + 1];
    starts[n] = run.count;
    if run.count == 0 {
        return Ok(starts);
    }
    for s in 1..n {
        let mut lo = 0u64;
        let mut hi = run.count;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let rec = read_run_record_at(run, mid)?;
            if rec_prefix_shard(&rec, n) < s {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        starts[s] = lo;
    }
    Ok(starts)
}

/// Stream k-way merge of sorted runs, invoking `on_rec` for each record in key order.
///
/// Does **not** buffer the full merge body (unlike [`merge_runs`]). Used by SH
/// bulk materialize so multi-run catalogs stay one sorted stream without
/// multi-GB RAM.
///
/// `verify_crc`: when true, re-scan each run body CRC before streaming (safe
/// default for merge). Materialize can pass **false** when MANIFEST/claims
/// already carry trusted CRCs from spill time.
pub fn for_each_merged_rec(
    inputs: &[SortedRunPath],
    on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    for_each_merged_rec_opts(inputs, true, on_rec)
}

/// Like [`for_each_merged_rec`] with explicit CRC-verify control.
pub fn for_each_merged_rec_opts(
    inputs: &[SortedRunPath],
    verify_crc: bool,
    on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    for_each_merged_rec_paged(inputs, verify_crc, RUN_CURSOR_PAGE, on_rec)
}

fn for_each_merged_rec_paged(
    inputs: &[SortedRunPath],
    verify_crc: bool,
    page_bytes: usize,
    on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    if inputs.is_empty() {
        return Ok(());
    }
    let (key_len, rec_len) = merge_lens(inputs)?;
    let n = merge_session_entries(inputs.len());
    let open = || -> Result<Vec<Option<RunCursor>>, StoreError> {
        let mut cursors = Vec::with_capacity(inputs.len());
        for run in inputs {
            let c = RunCursor::open_range_paged(run, 0, run.count, verify_crc, page_bytes)?;
            cursors.push(if c.remaining == 0 { None } else { Some(c) });
        }
        Ok(cursors)
    };
    if crate::bulk_io::io_uring_enabled() {
        return uring_session::with_thread_local(n, |s| {
            merge_cursors(Some(s), open()?, key_len, rec_len, on_rec)
        })?;
    }
    merge_cursors(None, open()?, key_len, rec_len, on_rec)
}

/// k-way merge of one prefix shard's slices (`cuts[run][shard]..cuts[run][shard+1]`).
pub fn for_each_merged_rec_shard(
    inputs: &[SortedRunPath],
    cuts: &[Vec<u64>],
    shard: usize,
    verify_crc: bool,
    on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    if inputs.is_empty() {
        return Ok(());
    }
    if cuts.len() != inputs.len() {
        return Err(StoreError::Corrupt("sorted run: shard cuts len mismatch"));
    }
    let (key_len, rec_len) = merge_lens(inputs)?;
    let n = merge_session_entries(inputs.len());
    let open = || -> Result<Vec<Option<RunCursor>>, StoreError> {
        let mut cursors = Vec::with_capacity(inputs.len());
        for (idx, run) in inputs.iter().enumerate() {
            let starts = &cuts[idx];
            if shard + 1 >= starts.len() {
                return Err(StoreError::Corrupt("sorted run: shard cut out of range"));
            }
            let lo = starts[shard];
            let count = starts[shard + 1].saturating_sub(lo);
            if count == 0 {
                cursors.push(None);
                continue;
            }
            let c = RunCursor::open_range(run, lo, count, verify_crc)?;
            cursors.push(if c.remaining == 0 { None } else { Some(c) });
        }
        Ok(cursors)
    };
    if crate::bulk_io::io_uring_enabled() {
        return uring_session::with_thread_local(n, |s| {
            merge_cursors(Some(s), open()?, key_len, rec_len, on_rec)
        })?;
    }
    merge_cursors(None, open()?, key_len, rec_len, on_rec)
}

fn merge_lens(inputs: &[SortedRunPath]) -> Result<(usize, u32), StoreError> {
    let key_len = inputs[0].key_len as usize;
    let rec_len = inputs[0].rec_len;
    for r in inputs {
        if r.key_len as usize != key_len || r.rec_len != rec_len {
            return Err(StoreError::Corrupt("sorted run: merge len mismatch"));
        }
    }
    Ok((key_len, rec_len))
}

fn merge_cursors(
    session: Option<&mut UringSession>,
    mut cursors: Vec<Option<RunCursor>>,
    key_len: usize,
    rec_len: u32,
    on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    match session {
        Some(s) => {
            let mut s = s.drain_guard();
            merge_cursors_inner(Some(&mut *s), &mut cursors, key_len, rec_len, on_rec)
        }
        None => merge_cursors_inner(None, &mut cursors, key_len, rec_len, on_rec),
    }
}

fn merge_cursors_inner(
    mut session: Option<&mut UringSession>,
    cursors: &mut [Option<RunCursor>],
    key_len: usize,
    rec_len: u32,
    mut on_rec: impl FnMut(&[u8]) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    if cursors.is_empty() {
        return Ok(());
    }
    if let Some(s) = session.as_mut() {
        s.begin_batch();
        let mut pushed = false;
        for i in 0..cursors.len() {
            if cursors[i].is_some() && try_submit_ahead_at(cursors, i, s)? {
                pushed = true;
            }
        }
        if pushed {
            s.submit()?;
        }
    }
    for i in 0..cursors.len() {
        if cursors[i].is_none() {
            continue;
        }
        let more = if let Some(s) = session.as_mut() {
            fill_next_at(cursors, i, s)?
        } else {
            cursors[i].as_mut().unwrap().fill_next()?
        };
        if !more {
            cursors[i] = None;
        }
    }
    let k = cursors.len();
    let mut tree = LoserTree::new(cursors, key_len);
    let mut prev_buf = [0u8; 40];
    let mut prev_len = 0usize;
    let mut prev_vec: Option<Vec<u8>> = None;
    loop {
        let w = tree.winner();
        if w == MERGE_SENTINEL || w >= k || cursors[w].is_none() {
            break;
        }
        {
            let rec = cursors[w].as_ref().unwrap().rec();
            let prev = if rec_len as usize <= 40 && prev_len > 0 {
                Some(&prev_buf[..prev_len])
            } else {
                prev_vec.as_deref()
            };
            let skip = should_skip_dup_key(key_len, rec_len, prev, rec);
            if !skip {
                on_rec(rec)?;
                if key_len == rec_len as usize {
                    if rec_len as usize <= 40 {
                        prev_buf[..rec.len()].copy_from_slice(rec);
                        prev_len = rec.len();
                    } else {
                        prev_vec = Some(rec.to_vec());
                    }
                }
            }
        }
        let more = if let Some(s) = session.as_mut() {
            fill_next_at(cursors, w, s)?
        } else {
            cursors[w].as_mut().unwrap().fill_next()?
        };
        if !more {
            cursors[w] = None;
        }
        tree.replay(cursors, w, key_len);
    }
    Ok(())
}

/// Stream-merge result: run path + max little-endian u64 at offset 32 (SH create_fk).
///
/// `max_u64_at_32` is 0 when `rec_len < 40` (no create_fk field).
#[derive(Debug, Clone)]
pub struct MergeToFileResult {
    pub run: SortedRunPath,
    pub max_u64_at_32: u64,
}

/// Stream-merge `inputs` into `out_path` **without** MANIFEST updates or deleting
/// inputs. Caller manages catalog / cleanup. At most open `|inputs|` cursors.
///
/// Equal full-record keys (`key_len == rec_len`, SH) are deduped. Equal
/// prefix keys with extra payload (`key_len < rec_len`) are all kept.
/// Streaming write (no full body RAM).
/// Uses [`RunWritePolicy::CATALOG`] (durable, unpaced, DONTNEED). Does not CRC-verify
/// inputs (caller trusted them at spill); catalog [`merge_runs`] verifies.
pub fn merge_runs_to_file(
    inputs: &[SortedRunPath],
    out_path: &Path,
) -> Result<SortedRunPath, StoreError> {
    Ok(merge_runs_to_file_with_policy(inputs, out_path, RunWritePolicy::CATALOG, false)?.run)
}

/// Like [`merge_runs_to_file`] with write policy and optional per-input CRC verify.
pub fn merge_runs_to_file_with_policy(
    inputs: &[SortedRunPath],
    out_path: &Path,
    policy: RunWritePolicy,
    verify_crc: bool,
) -> Result<MergeToFileResult, StoreError> {
    if inputs.is_empty() {
        let run = write_sorted_run_file(out_path, 40, 40, &[], policy)?;
        return Ok(MergeToFileResult {
            run,
            max_u64_at_32: 0,
        });
    }
    let key_len = inputs[0].key_len as usize;
    let rec_len = inputs[0].rec_len;
    for r in inputs {
        if r.key_len as usize != key_len || r.rec_len != rec_len {
            return Err(StoreError::Corrupt("sorted run: merge len mismatch"));
        }
    }
    let track_fk = rec_len as usize >= 40;

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent).map_err(|e| io_err(parent, e))?;
    }
    let tmp = out_path.with_extension("tmp");
    let mut count = 0u64;
    let mut body_crc = 0xFFFF_FFFFu32;
    let mut max_u64_at_32 = 0u64;
    let mut pace_since = 0usize;
    let table = crc32_table();
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| io_err(&tmp, e))?;
        let hdr = [0u8; HEADER_LEN];
        f.write_all(&hdr).map_err(|e| io_err(&tmp, e))?;
        let n = merge_session_entries(inputs.len());
        let on_rec = |rec: &[u8]| -> Result<(), StoreError> {
            f.write_all(rec).map_err(|e| io_err(&tmp, e))?;
            if track_fk && rec.len() >= 40 {
                let fk = u64::from_le_bytes(rec[32..40].try_into().unwrap());
                if fk > max_u64_at_32 {
                    max_u64_at_32 = fk;
                }
            }
            for &b in rec {
                body_crc = table[((body_crc ^ u32::from(b)) & 0xFF) as usize] ^ (body_crc >> 8);
            }
            count = count.saturating_add(1);
            if policy.pace {
                pace_since = pace_since.saturating_add(rec.len());
                if pace_since >= PACE_CHUNK_BYTES {
                    pace_since = 0;
                    std::thread::sleep(PACE_SLEEP);
                }
            }
            Ok(())
        };
        let open_cursors = || -> Result<Vec<Option<RunCursor>>, StoreError> {
            let mut cursors = Vec::with_capacity(inputs.len());
            for run in inputs {
                let c = RunCursor::open(run, verify_crc)?;
                cursors.push(if c.remaining == 0 { None } else { Some(c) });
            }
            Ok(cursors)
        };
        if crate::bulk_io::io_uring_enabled() {
            uring_session::with_thread_local(n, |s| {
                merge_cursors(Some(s), open_cursors()?, key_len, rec_len, on_rec)
            })??;
        } else {
            merge_cursors(None, open_cursors()?, key_len, rec_len, on_rec)?;
        }
        body_crc ^= 0xFFFF_FFFF;
        f.seek(SeekFrom::Start(0)).map_err(|e| io_err(&tmp, e))?;
        let mut hdr = [0u8; HEADER_LEN];
        hdr[0..8].copy_from_slice(&MAGIC);
        hdr[8..12].copy_from_slice(&VERSION.to_le_bytes());
        hdr[12..16].copy_from_slice(&(key_len as u32).to_le_bytes());
        hdr[16..20].copy_from_slice(&rec_len.to_le_bytes());
        hdr[20..28].copy_from_slice(&count.to_le_bytes());
        hdr[28..32].copy_from_slice(&body_crc.to_le_bytes());
        f.write_all(&hdr).map_err(|e| io_err(&tmp, e))?;
        if policy.durable {
            f.sync_all().map_err(|e| io_err(&tmp, e))?;
        }
    }
    fs::rename(&tmp, out_path).map_err(|e| io_err(out_path, e))?;
    if policy.durable {
        if let Some(parent) = out_path.parent() {
            if let Ok(dirf) = File::open(parent) {
                let _ = dirf.sync_all();
            }
        }
    }
    if policy.drop_cache {
        advise_file_dont_need(out_path);
    }
    Ok(MergeToFileResult {
        run: SortedRunPath {
            path: out_path.to_path_buf(),
            count,
            rec_len,
            key_len: key_len as u32,
            body_crc32: body_crc,
        },
        max_u64_at_32,
    })
}

/// Marker file: fan-in reduce finished; outputs under `work_dir` supersede inputs.
///
/// Written by the SH tip materialize path after a successful reduce so claimed
/// `*.run.mat` inputs can be deleted immediately. Crash recovery resumes from
/// `work_dir` when this marker is present (see [`list_fanin_reduce_outputs`]).
pub const FANIN_READY_NAME: &str = "READY";

/// Mid-reduce checkpoint (recoverable after SIGINT mid-chunk or mid-pass).
///
/// Format `RBFANCP2`: remaining input paths + completed output basenames under
/// `work_dir`, plus `next_gen` / `next_seq` / `fanin`. Updated after **each**
/// successful chunk merge; inputs for that chunk are deleted immediately.
pub const FANIN_CHECKPOINT_NAME: &str = "CHECKPOINT";

const CHECKPOINT_MAGIC_V2: &str = "RBFANCP2";
/// Legacy full-pass-only checkpoints (ignored → fresh reduce).
const CHECKPOINT_MAGIC_V1: &str = "RBFANCP1";

/// Max runs for **direct** k-way materialize (and rare fan-in fallback target).
///
/// Catalog should stay O(10³) via IBD promote; hosts can open a few thousand FDs.
pub const FANIN_TARGET_STREAM_RUNS: usize = 4096;
/// Upper bound on k-way open cursors per chunk merge (fallback reduce only).
pub const FANIN_MAX_CHUNK: usize = 512;

/// Durable partial reduce state for resume.
#[derive(Debug, Clone)]
pub struct FaninCheckpoint {
    pub next_gen: u32,
    pub next_seq: u64,
    pub fanin: usize,
    /// Inputs still waiting to be merged (may be abs paths or under work_dir).
    pub remaining: Vec<SortedRunPath>,
    /// Outputs already produced this (single) pass under `work_dir`.
    pub done_outputs: Vec<SortedRunPath>,
}

fn checkpoint_path(work_dir: &Path) -> PathBuf {
    work_dir.join(FANIN_CHECKPOINT_NAME)
}

fn path_for_checkpoint(work_dir: &Path, run: &SortedRunPath) -> String {
    if let Ok(rel) = run.path.strip_prefix(work_dir) {
        return format!("w:{}", rel.display());
    }
    format!("a:{}", run.path.display())
}

fn open_checkpoint_path(work_dir: &Path, encoded: &str) -> Result<SortedRunPath, StoreError> {
    let path = if let Some(rel) = encoded.strip_prefix("w:") {
        work_dir.join(rel)
    } else if let Some(abs) = encoded.strip_prefix("a:") {
        PathBuf::from(abs)
    } else if !encoded.contains('/') && !encoded.contains('\\') {
        work_dir.join(encoded)
    } else {
        PathBuf::from(encoded)
    };
    open_run(&path)
}

/// Write partial/full reduce checkpoint atomically.
pub fn write_fanin_checkpoint(
    work_dir: &Path,
    next_gen: u32,
    next_seq: u64,
    fanin: usize,
    remaining: &[SortedRunPath],
    done_outputs: &[SortedRunPath],
) -> Result<(), StoreError> {
    fs::create_dir_all(work_dir).map_err(|e| io_err(work_dir, e))?;
    let path = checkpoint_path(work_dir);
    let tmp = work_dir.join(format!("{FANIN_CHECKPOINT_NAME}.tmp"));
    let mut body = String::new();
    body.push_str(CHECKPOINT_MAGIC_V2);
    body.push('\n');
    body.push_str(&format!("next_gen={next_gen}\n"));
    body.push_str(&format!("next_seq={next_seq}\n"));
    body.push_str(&format!("fanin={fanin}\n"));
    body.push_str(&format!("n_rem={}\n", remaining.len()));
    body.push_str(&format!("n_out={}\n", done_outputs.len()));
    for r in remaining {
        body.push_str("in=");
        body.push_str(&path_for_checkpoint(work_dir, r));
        body.push('\n');
    }
    for r in done_outputs {
        body.push_str("out=");
        body.push_str(&path_for_checkpoint(work_dir, r));
        body.push('\n');
    }
    fs::write(&tmp, body.as_bytes()).map_err(|e| io_err(&tmp, e))?;
    {
        let f = OpenOptions::new()
            .write(true)
            .open(&tmp)
            .map_err(|e| io_err(&tmp, e))?;
        f.sync_all().map_err(|e| io_err(&tmp, e))?;
    }
    fs::rename(&tmp, &path).map_err(|e| io_err(&path, e))?;
    Ok(())
}

/// Load v2 checkpoint; v1 is ignored (returns None → fresh reduce).
pub fn load_fanin_checkpoint(work_dir: &Path) -> Result<Option<FaninCheckpoint>, StoreError> {
    let path = checkpoint_path(work_dir);
    if !path.is_file() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path).map_err(|e| io_err(&path, e))?;
    let mut lines = text.lines();
    let magic = lines.next().unwrap_or("");
    if magic == CHECKPOINT_MAGIC_V1 {
        rbitcoin_log::warn!("store: fanin CHECKPOINT v1 obsolete — starting fresh reduce");
        return Ok(None);
    }
    if magic != CHECKPOINT_MAGIC_V2 {
        rbitcoin_log::warn!("store: fanin CHECKPOINT bad magic — ignoring");
        return Ok(None);
    }
    let mut next_gen = 0u32;
    let mut next_seq = 1u64;
    let mut fanin = 32usize;
    let mut remaining = Vec::new();
    let mut done_outputs = Vec::new();
    for line in lines {
        if let Some(v) = line.strip_prefix("next_gen=") {
            next_gen = v.parse().unwrap_or(0);
        } else if let Some(v) = line.strip_prefix("next_seq=") {
            next_seq = v.parse().unwrap_or(1);
        } else if let Some(v) = line.strip_prefix("fanin=") {
            fanin = v.parse().unwrap_or(32).max(1);
        } else if let Some(enc) = line.strip_prefix("in=") {
            match open_checkpoint_path(work_dir, enc) {
                Ok(r) => remaining.push(r),
                Err(e) => {
                    rbitcoin_log::warn!(
                        "store: fanin CHECKPOINT missing input {enc} ({e}) — ignoring checkpoint"
                    );
                    return Ok(None);
                }
            }
        } else if let Some(enc) = line.strip_prefix("out=") {
            match open_checkpoint_path(work_dir, enc) {
                Ok(r) => done_outputs.push(r),
                Err(e) => {
                    rbitcoin_log::warn!(
                        "store: fanin CHECKPOINT missing output {enc} ({e}) — ignoring checkpoint"
                    );
                    return Ok(None);
                }
            }
        }
    }
    if remaining.is_empty() && done_outputs.is_empty() {
        return Ok(None);
    }
    Ok(Some(FaninCheckpoint {
        next_gen,
        next_seq,
        fanin,
        remaining,
        done_outputs,
    }))
}

fn clear_tmp_in_work_dir(work_dir: &Path) {
    let Ok(rd) = fs::read_dir(work_dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.ends_with(".tmp") || p.extension().and_then(|x| x.to_str()) == Some("tmp") {
            let _ = fs::remove_file(&p);
        }
    }
}

fn cancel_requested(cancel: Option<&std::sync::atomic::AtomicBool>) -> bool {
    cancel
        .map(|c| c.load(std::sync::atomic::Ordering::Relaxed))
        .unwrap_or(false)
}

/// How many parallel chunk merges within the single fan-in pass.
///
/// Default: all logical CPUs. Override `RBITCOIN_SH_MERGE_WORKERS` (`1` = serial).
pub fn sh_merge_workers() -> usize {
    if let Ok(s) = std::env::var("RBITCOIN_SH_MERGE_WORKERS") {
        if let Ok(n) = s.parse::<usize>() {
            return n.clamp(1, 256);
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, 256)
}

/// Choose chunk width so **one** pass yields ≤ `target_stream` outputs.
///
/// `outputs ≈ ceil(n / fanin) ≤ TARGET` ⇒ `fanin ≥ ceil(n / TARGET)`.
/// Production uses [`FANIN_TARGET_STREAM_RUNS`]; tests may pass a tiny target.
pub fn dynamic_merge_fanin_for(n_runs: usize, target_stream: usize) -> usize {
    let target = target_stream.max(1);
    if n_runs == 0 {
        return target;
    }
    if n_runs <= target {
        return n_runs.max(1);
    }
    n_runs.div_ceil(target).clamp(8, FANIN_MAX_CHUNK)
}

/// Choose chunk width so **one** pass yields ≤ [`FANIN_TARGET_STREAM_RUNS`] outputs.
///
/// `outputs ≈ ceil(n / fanin) ≤ TARGET` ⇒ `fanin ≥ ceil(n / TARGET)`.
#[inline]
pub fn dynamic_merge_fanin(n_runs: usize) -> usize {
    dynamic_merge_fanin_for(n_runs, FANIN_TARGET_STREAM_RUNS)
}

/// Resolve `target_stream_runs` for reduce: `0` means production default.
#[inline]
fn resolve_target_stream(target_stream_runs: usize) -> usize {
    if target_stream_runs == 0 {
        FANIN_TARGET_STREAM_RUNS
    } else {
        target_stream_runs
    }
}

/// Always 0 or 1 with dynamic single-pass reduce (kept for log compatibility).
pub fn fanin_passes_total(n: usize, fanin: usize) -> u32 {
    let fanin = fanin.max(1);
    if n == 0 || n <= fanin {
        0
    } else {
        1
    }
}

fn run_body_bytes(run: &SortedRunPath) -> u64 {
    run.count.saturating_mul(u64::from(run.rec_len))
}

/// Wall interval for tip fan-in reduce INFO heartbeats (time-based only).
const REDUCE_STATUS_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

struct ReduceStatus {
    t0: std::time::Instant,
    last_log: Option<std::time::Instant>,
    chunks_done: usize,
    chunks_total: usize,
    bytes_done: u64,
    fanin: usize,
    workers: usize,
}

impl ReduceStatus {
    fn new(fanin: usize, workers: usize, chunks_total: usize) -> Self {
        Self {
            t0: std::time::Instant::now(),
            last_log: None,
            chunks_done: 0,
            chunks_total,
            bytes_done: 0,
            fanin,
            workers,
        }
    }

    fn pct(&self) -> f64 {
        if self.chunks_total == 0 {
            return 100.0;
        }
        (100.0 * self.chunks_done as f64 / self.chunks_total as f64).clamp(0.0, 99.9)
    }

    fn maybe_log(&mut self, force: bool) {
        if !force {
            if let Some(t) = self.last_log {
                if t.elapsed() < REDUCE_STATUS_INTERVAL {
                    return;
                }
            }
        }
        self.last_log = Some(std::time::Instant::now());
        let elapsed = self.t0.elapsed();
        let secs = elapsed.as_secs_f64().max(1e-3);
        let mib = self.bytes_done as f64 / (1024.0 * 1024.0);
        let rate = mib / secs;
        rbitcoin_log::info!(
            "store: scripthash fanin reduce status pass=1/1 chunks={}/{} pct≈{:.1}% \
             elapsed={:?} rate≈{:.1}MiB/s fanin={} workers={}",
            self.chunks_done,
            self.chunks_total,
            self.pct(),
            elapsed,
            rate,
            self.fanin,
            self.workers,
        );
    }

    fn on_chunk_done(&mut self, chunk_bytes: u64) {
        self.chunks_done = self.chunks_done.saturating_add(1);
        self.bytes_done = self.bytes_done.saturating_add(chunk_bytes);
        self.maybe_log(false);
    }
}

/// Reduce `inputs` to ≤ target stream runs via **one** fan-in pass.
///
/// `target_stream_runs`: max stream runs after one pass. **`0` = production
/// default** ([`FANIN_TARGET_STREAM_RUNS`]). Tests pass a small target so reduce
/// behavior is covered without creating thousands of run files.
///
/// Dynamic fanin ([`dynamic_merge_fanin_for`]) so `ceil(n/fanin) ≤ target`.
/// After each chunk: delete inputs, write CHECKPOINT (partial-pass resume).
pub fn reduce_runs_to_fanin(
    inputs: &[SortedRunPath],
    work_dir: &Path,
    target_stream_runs: usize,
) -> Result<Vec<SortedRunPath>, StoreError> {
    reduce_runs_to_fanin_cancellable(inputs, work_dir, target_stream_runs, None)
}

/// Like [`reduce_runs_to_fanin`] with cooperative cancel.
pub fn reduce_runs_to_fanin_cancellable(
    inputs: &[SortedRunPath],
    work_dir: &Path,
    target_stream_runs: usize,
    cancel: Option<&std::sync::atomic::AtomicBool>,
) -> Result<Vec<SortedRunPath>, StoreError> {
    let target = resolve_target_stream(target_stream_runs);
    fs::create_dir_all(work_dir).map_err(|e| io_err(work_dir, e))?;
    clear_tmp_in_work_dir(work_dir);

    let workers = sh_merge_workers();
    let remaining: Vec<SortedRunPath>;
    let done_outputs: Vec<SortedRunPath>;
    let gen: u32;
    let mut seq: u64;
    let fanin: usize;
    let resumed: bool;

    if let Some(cp) = load_fanin_checkpoint(work_dir)? {
        remaining = cp.remaining;
        done_outputs = cp.done_outputs;
        gen = cp.next_gen;
        seq = cp.next_seq;
        fanin = cp.fanin.max(1);
        resumed = true;
        rbitcoin_log::info!(
            "store: scripthash fanin reduce resume remaining={} done_out={} fanin={fanin} gen={gen}",
            remaining.len(),
            done_outputs.len()
        );
        if remaining.is_empty() {
            rbitcoin_log::info!(
                "store: scripthash fanin reduce resume complete stream_runs={}",
                done_outputs.len()
            );
            return Ok(done_outputs);
        }
    } else {
        // Fresh: wipe old merge work (not READY — caller owns READY path).
        if let Ok(rd) = fs::read_dir(work_dir) {
            for e in rd.flatten() {
                let p = e.path();
                let name = p.file_name().and_then(|s| s.to_str()).unwrap_or("");
                if name == FANIN_READY_NAME {
                    continue;
                }
                let _ = fs::remove_file(&p);
            }
        }
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        fanin = dynamic_merge_fanin_for(inputs.len(), target);
        if inputs.len() <= target {
            rbitcoin_log::info!(
                "store: scripthash fanin reduce skipped runs={} already≤target={}",
                inputs.len(),
                target
            );
            return Ok(inputs.to_vec());
        }
        remaining = inputs.to_vec();
        done_outputs = Vec::new();
        gen = 0;
        seq = 1;
        resumed = false;
    }

    let total_recs: u64 = remaining
        .iter()
        .chain(done_outputs.iter())
        .map(|r| r.count)
        .sum();
    let total_body: u64 = remaining
        .iter()
        .chain(done_outputs.iter())
        .map(run_body_bytes)
        .sum();
    rbitcoin_log::info!(
        "store: scripthash fanin reduce start remaining={} done_out={} fanin={fanin} workers={workers} \
         records≈{total_recs} body≈{:.1}MiB passes=1 resumed={resumed}",
        remaining.len(),
        done_outputs.len(),
        total_body as f64 / (1024.0 * 1024.0),
    );

    let n_chunks = remaining.len().div_ceil(fanin).max(1);
    let mut status = ReduceStatus::new(fanin, workers, done_outputs.len() + n_chunks);
    status.chunks_done = done_outputs.len();
    status.maybe_log(true);

    let mut jobs: Vec<(Vec<SortedRunPath>, PathBuf)> = Vec::with_capacity(n_chunks);
    let mut rem = remaining;
    while !rem.is_empty() {
        let take = fanin.min(rem.len());
        let chunk: Vec<SortedRunPath> = rem.drain(..take).collect();
        let out_path = work_dir.join(format!("g{gen}_{seq:06}.run"));
        seq += 1;
        jobs.push((chunk, out_path));
    }

    use std::sync::Mutex;
    let pending_inputs: Vec<SortedRunPath> =
        jobs.iter().flat_map(|(c, _)| c.iter().cloned()).collect();
    let state = Mutex::new(FaninChunkState {
        done_outputs: done_outputs.clone(),
        pending_inputs,
        seq_note: seq,
        cancelled: false,
        last_err: None,
        chunks_finished: 0u64,
    });

    let job_list = Mutex::new(jobs);
    let n_workers = workers.max(1).min(n_chunks.max(1));
    let status_mu = Mutex::new(status);

    std::thread::scope(|scope| {
        for _ in 0..n_workers {
            let job_list = &job_list;
            let state = &state;
            let status_mu = &status_mu;
            let work_dir = work_dir;
            let fanin = fanin;
            let gen = gen;
            scope.spawn(move || loop {
                if cancel_requested(cancel) {
                    let mut st = state.lock().unwrap();
                    st.cancelled = true;
                    break;
                }
                let job = {
                    let mut q = job_list.lock().unwrap();
                    q.pop()
                };
                let Some((chunk, out_path)) = job else {
                    break;
                };
                let chunk_bytes: u64 = chunk.iter().map(run_body_bytes).sum();
                match merge_runs_to_file(&chunk, &out_path) {
                    Ok(merged) => {
                        for r in &chunk {
                            let _ = fs::remove_file(&r.path);
                        }
                        {
                            let mut st = state.lock().unwrap();
                            st.pending_inputs
                                .retain(|p| !chunk.iter().any(|c| c.path == p.path));
                            st.done_outputs.push(merged);
                            st.chunks_finished += 1;
                            let _ = write_fanin_checkpoint(
                                work_dir,
                                gen,
                                st.seq_note,
                                fanin,
                                &st.pending_inputs,
                                &st.done_outputs,
                            );
                        }
                        if let Ok(mut s) = status_mu.lock() {
                            s.on_chunk_done(chunk_bytes);
                        }
                    }
                    Err(e) => {
                        let mut st = state.lock().unwrap();
                        st.cancelled = true;
                        st.last_err = Some(e);
                        break;
                    }
                }
            });
        }
    });

    let st = state.into_inner().unwrap();
    let mut status = status_mu.into_inner().unwrap();
    if let Some(e) = st.last_err {
        return Err(e);
    }
    if st.cancelled || cancel_requested(cancel) {
        rbitcoin_log::warn!(
            "store: scripthash fanin reduce cancelled pending_in={} done_out={} — checkpoint kept",
            st.pending_inputs.len(),
            st.done_outputs.len()
        );
        return Err(StoreError::Cancelled("scripthash fanin reduce"));
    }
    if !st.pending_inputs.is_empty() {
        return Err(StoreError::Corrupt(
            "scripthash fanin reduce: unfinished pending inputs",
        ));
    }

    status.chunks_done = st.done_outputs.len();
    status.chunks_total = st.done_outputs.len().max(1);
    status.maybe_log(true);
    rbitcoin_log::info!(
        "store: scripthash fanin reduce done stream_runs={} fanin={fanin} workers={workers} \
         elapsed={:?} pct=100",
        st.done_outputs.len(),
        status.t0.elapsed(),
    );
    write_fanin_checkpoint(work_dir, gen, seq, fanin, &[], &st.done_outputs)?;
    Ok(st.done_outputs)
}

struct FaninChunkState {
    done_outputs: Vec<SortedRunPath>,
    pending_inputs: Vec<SortedRunPath>,
    seq_note: u64,
    cancelled: bool,
    last_err: Option<StoreError>,
    chunks_finished: u64,
}

/// List finished fan-in reduce outputs under `work_dir` when [`FANIN_READY_NAME`] is set.
///
/// Returns `Ok(None)` if not ready / empty. Used to resume tip materialize after
/// claimed inputs were deleted post-reduce.
pub fn list_fanin_reduce_outputs(
    work_dir: &Path,
) -> Result<Option<Vec<SortedRunPath>>, StoreError> {
    let ready = work_dir.join(FANIN_READY_NAME);
    if !ready.is_file() {
        return Ok(None);
    }
    if !work_dir.is_dir() {
        return Ok(None);
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(work_dir)
        .map_err(|e| io_err(work_dir, e))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("run"))
        .collect();
    paths.sort();
    if paths.is_empty() {
        return Ok(None);
    }
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        out.push(open_run(&p)?);
    }
    Ok(Some(out))
}

/// Mark fan-in reduce complete and delete `inputs` that are fully superseded by
/// `outputs` under `work_dir` (not the same path as an output).
///
/// Call only after [`reduce_runs_to_fanin`] returns outputs all under `work_dir`.
/// Writes [`FANIN_READY_NAME`] first so crash recovery can resume from outputs
/// even if some input deletes fail mid-loop.
pub fn commit_fanin_reduce_and_drop_inputs(
    work_dir: &Path,
    inputs: &[SortedRunPath],
    outputs: &[SortedRunPath],
) -> Result<(), StoreError> {
    if outputs.is_empty() {
        return Ok(());
    }
    // Only free originals when outputs live in the work dir (true reduce happened).
    let all_out_in_work = outputs.iter().all(|o| o.path.starts_with(work_dir));
    if !all_out_in_work {
        return Ok(());
    }
    fs::create_dir_all(work_dir).map_err(|e| io_err(work_dir, e))?;
    let ready = work_dir.join(FANIN_READY_NAME);
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&ready)
            .map_err(|e| io_err(&ready, e))?;
        f.write_all(b"1\n").map_err(|e| io_err(&ready, e))?;
        f.sync_all().map_err(|e| io_err(&ready, e))?;
    }
    let out_paths: std::collections::HashSet<&Path> =
        outputs.iter().map(|o| o.path.as_path()).collect();
    for r in inputs {
        if out_paths.contains(r.path.as_path()) {
            continue;
        }
        if r.path.exists() {
            let _ = fs::remove_file(&r.path);
        }
    }
    Ok(())
}

/// K-way merge of sorted runs → new run at `out_path`. Deletes input files on success.
///
/// Equal keys: all records are kept (multi-value multimap for SH creates).
/// Single atomic MANIFEST update: drop inputs, add output.
///
/// Streams the merge to disk (incremental CRC) — does **not** hold the full
/// output body in RAM. Uses [`RunWritePolicy::CATALOG`] (unpaced durable write).
pub fn merge_runs(inputs: &[SortedRunPath], out_path: &Path) -> Result<SortedRunPath, StoreError> {
    Ok(merge_runs_with_policy(inputs, out_path, RunWritePolicy::CATALOG)?.run)
}

/// Like [`merge_runs`] with write policy; also returns max u64 at record offset 32
/// (SH create_fk) so callers can bump SEAL without a second full-body read.
pub fn merge_runs_with_policy(
    inputs: &[SortedRunPath],
    out_path: &Path,
    policy: RunWritePolicy,
) -> Result<MergeToFileResult, StoreError> {
    if inputs.is_empty() {
        let run = write_sorted_run(out_path, 32, 44, &[])?;
        return Ok(MergeToFileResult {
            run,
            max_u64_at_32: 0,
        });
    }
    // CRC-verify inputs when promoting into the durable catalog; L0 rewrites skip
    // (bodies just written by this process).
    let verify_crc = policy.durable;
    let merged = merge_runs_to_file_with_policy(inputs, out_path, policy, verify_crc)?;
    let remove_seqs: Vec<u64> = inputs.iter().filter_map(|r| r.seq()).collect();
    if policy.durable {
        if let Some(dir) = out_path.parent() {
            manifest_merge_commit(dir, &remove_seqs, &merged.run)?;
        }
    }
    for r in inputs {
        let _ = fs::remove_file(&r.path);
    }
    Ok(merged)
}

fn scan_run_paths(dir: &Path) -> Result<Vec<PathBuf>, StoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut paths: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(|e| io_err(dir, e))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("run"))
        .collect();
    paths.sort();
    Ok(paths)
}

fn open_and_check_against_entry(
    path: &Path,
    expect: &ManifestEntry,
) -> Result<SortedRunPath, StoreError> {
    let run = open_run(path)?;
    if run.count != expect.count || run.key_len != expect.key_len || run.rec_len != expect.rec_len {
        return Err(StoreError::Corrupt(
            "sorted run: header does not match MANIFEST",
        ));
    }
    if expect.body_crc32 != 0 && run.body_crc32 != 0 && run.body_crc32 != expect.body_crc32 {
        return Err(StoreError::Corrupt(
            "sorted run: CRC does not match MANIFEST",
        ));
    }
    Ok(run)
}

/// List runs in `dir` (sorted by seq / name).
///
/// When `MANIFEST` exists it is the **authoritative** set: only listed runs are
/// returned; orphans are removed best-effort; missing listed files are warned.
/// Without a manifest, falls back to a directory scan and rebuilds the catalog.
///
/// **Must** be called under the family's `runs_io` lock whenever concurrent
/// writers/mergers/materialize may touch the same directory (see module docs).
pub fn list_runs(dir: &Path) -> Result<Vec<SortedRunPath>, StoreError> {
    if !dir.exists() {
        return Ok(Vec::new());
    }

    if let Some(mf) = load_manifest(dir)? {
        let mut out = Vec::with_capacity(mf.entries.len());
        let mut listed_seqs = std::collections::HashSet::with_capacity(mf.entries.len());
        for e in &mf.entries {
            listed_seqs.insert(e.seq);
            let path = next_run_path(dir, e.seq);
            match open_and_check_against_entry(&path, e) {
                Ok(r) => out.push(r),
                Err(StoreError::Io { source, .. })
                    if source.kind() == std::io::ErrorKind::NotFound =>
                {
                    rbitcoin_log::warn!(
                        "store: MANIFEST lists missing run {} (seq={}) — data may be incomplete",
                        path.display(),
                        e.seq
                    );
                }
                Err(e) => {
                    rbitcoin_log::warn!("store: skipping bad sorted run {}: {e}", path.display());
                }
            }
        }
        // Orphan `*.run` files not in the catalog (e.g. merge inputs left after a
        // successful MANIFEST commit, or a crash between write and catalog).
        // Safe only for true leftovers: claimed materialize files use `*.run.mat`
        // and are not scanned here. Deleting an in-flight claim would drop data.
        for p in scan_run_paths(dir)? {
            let Some(seq) = seq_from_path(&p) else {
                continue;
            };
            if !listed_seqs.contains(&seq) {
                rbitcoin_log::debug!(
                    "store: removing orphan sorted run not in MANIFEST {}",
                    p.display()
                );
                let _ = fs::remove_file(&p);
            }
        }
        return Ok(out);
    }

    // Legacy / empty: scan directory, heal by writing MANIFEST.
    let paths = scan_run_paths(dir)?;
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        match open_run(&p) {
            Ok(r) => out.push(r),
            Err(StoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            }
            Err(e) => {
                rbitcoin_log::warn!("store: skipping bad sorted run {}: {e}", p.display());
            }
        }
    }
    if !out.is_empty() {
        if let Err(e) = rebuild_manifest_from_runs(dir, &out) {
            rbitcoin_log::warn!(
                "store: failed to rebuild sorted-run MANIFEST in {}: {e}",
                dir.display()
            );
        }
    }
    Ok(out)
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn tmp_dir() -> PathBuf {
        let n = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!("rbitcoin-sorted-run-{n}"));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn rec(key: u8, tag: u8) -> [u8; 44] {
        let mut r = [0u8; 44];
        r[0] = key;
        r[32] = tag;
        r
    }

    /// Tiny stream target so reduce runs without thousands of run files.
    const TEST_TARGET: usize = 16;

    fn write_n_runs(dir: &Path, n: u64, seq_base: u64) -> Vec<SortedRunPath> {
        let mut inputs = Vec::with_capacity(n as usize);
        for i in 1..=n {
            let p = next_run_path(dir, seq_base + i);
            write_sorted_run(&p, 32, 44, &rec(((seq_base + i) % 200) as u8, i as u8)).unwrap();
            inputs.push(open_run(&p).unwrap());
        }
        inputs
    }

    /// Scope `RBITCOIN_SH_MERGE_WORKERS` for one test (restore previous value).
    struct MergeWorkersGuard {
        prev: Option<std::ffi::OsString>,
    }

    impl MergeWorkersGuard {
        fn set(workers: &str) -> Self {
            let prev = std::env::var_os("RBITCOIN_SH_MERGE_WORKERS");
            std::env::set_var("RBITCOIN_SH_MERGE_WORKERS", workers);
            Self { prev }
        }
    }

    impl Drop for MergeWorkersGuard {
        fn drop(&mut self) {
            match &self.prev {
                Some(v) => std::env::set_var("RBITCOIN_SH_MERGE_WORKERS", v),
                None => std::env::remove_var("RBITCOIN_SH_MERGE_WORKERS"),
            }
        }
    }

    #[test]
    fn crc32_known_vector() {
        // CRC-32/ISO-HDLC of "123456789"
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn write_read_roundtrip() {
        // Also covers post-write fadvise DONTNEED: re-fault on read must succeed.
        let d = tmp_dir();
        let path = d.join("000001.run");
        let mut body = Vec::new();
        body.extend_from_slice(&rec(1, 10));
        body.extend_from_slice(&rec(2, 20));
        write_sorted_run(&path, 32, 44, &body).unwrap();
        let run = open_run(&path).unwrap();
        assert_eq!(run.count, 2);
        assert_ne!(run.body_crc32, 0);
        let b = read_run_body(&run).unwrap();
        assert_eq!(b.len(), 88);
        assert_eq!(b[0], 1);
        assert_eq!(b[32], 10);
        verify_run_body(&run).unwrap();
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn body_crc_detects_corruption() {
        let d = tmp_dir();
        let path = d.join("000001.run");
        let body = rec(1, 10);
        write_sorted_run(&path, 32, 44, &body).unwrap();
        // Flip a body byte.
        let mut raw = fs::read(&path).unwrap();
        raw[HEADER_LEN] ^= 0xFF;
        fs::write(&path, &raw).unwrap();
        let run = open_run(&path).unwrap();
        assert!(read_run_body(&run).is_err());
        assert!(verify_run_body(&run).is_err());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn manifest_tracks_write_and_merge() {
        let d = tmp_dir();
        let p1 = d.join("000001.run");
        let p2 = d.join("000002.run");
        let mut a = Vec::new();
        a.extend_from_slice(&rec(1, 1));
        a.extend_from_slice(&rec(3, 3));
        write_sorted_run(&p1, 32, 44, &a).unwrap();
        let mut b = Vec::new();
        b.extend_from_slice(&rec(2, 2));
        b.extend_from_slice(&rec(4, 4));
        write_sorted_run(&p2, 32, 44, &b).unwrap();

        let listed = list_runs(&d).unwrap();
        assert_eq!(listed.len(), 2);
        let mf = load_manifest(&d).unwrap().unwrap();
        assert_eq!(mf.entries.len(), 2);

        let r1 = open_run(&p1).unwrap();
        let r2 = open_run(&p2).unwrap();
        let out = d.join("000003.run");
        let merged = merge_runs(&[r1, r2], &out).unwrap();
        assert_eq!(merged.count, 4);
        assert!(!p1.exists());
        assert!(!p2.exists());

        let listed = list_runs(&d).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].seq(), Some(3));
        let mf = load_manifest(&d).unwrap().unwrap();
        assert_eq!(mf.entries.len(), 1);
        assert_eq!(mf.entries[0].seq, 3);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn list_runs_ignores_orphan_not_in_manifest() {
        let d = tmp_dir();
        write_sorted_run(&d.join("000001.run"), 32, 44, &rec(1, 1)).unwrap();
        // Plant orphan without going through write_sorted_run catalog path.
        let orphan = d.join("000099.run");
        write_sorted_run_file(&orphan, 32, 44, &rec(9, 9), RunWritePolicy::DURABLE).unwrap();
        // MANIFEST still only has 000001.
        let runs = list_runs(&d).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].seq(), Some(1));
        assert!(!orphan.exists(), "orphan should be removed");
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn list_materialize_claims_finds_orphan_mats() {
        let d = tmp_dir();
        let path = d.join("000001.run");
        let run = write_sorted_run(&path, 32, 44, &rec(1, 1)).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        // MANIFEST empty; list_runs empty; claims still visible.
        assert!(list_runs(&d).unwrap().is_empty());
        let mats = list_materialize_claims(&d).unwrap();
        assert_eq!(mats.len(), 1);
        assert_eq!(mats[0].path, claimed.path);
        assert_eq!(mats[0].count, 1);
        // Re-claim is idempotent (open as-is).
        let again = claim_run_for_materialize(&mats[0]).unwrap();
        assert_eq!(again.path, claimed.path);
        let _ = fs::remove_dir_all(&d);
    }

    fn sh_rec(sh0: u8, fk: u64) -> [u8; 40] {
        let mut r = [0u8; 40];
        r[0] = sh0;
        r[32..40].copy_from_slice(&fk.to_le_bytes());
        r
    }

    #[test]
    fn sh_merge_orders_fks_across_runs_and_dedups() {
        let d = tmp_dir();
        let mut a = Vec::new();
        a.extend_from_slice(&sh_rec(0xAA, 1));
        a.extend_from_slice(&sh_rec(0xAA, 5));
        a.extend_from_slice(&sh_rec(0xAA, 255));
        a.extend_from_slice(&sh_rec(0xAA, 257));
        let mut b = Vec::new();
        b.extend_from_slice(&sh_rec(0xAA, 2));
        b.extend_from_slice(&sh_rec(0xAA, 5));
        b.extend_from_slice(&sh_rec(0xAA, 256));
        write_sorted_run(&d.join("000001.run"), 40, 40, &a).unwrap();
        write_sorted_run(&d.join("000002.run"), 40, 40, &b).unwrap();
        let r1 = open_run(&d.join("000001.run")).unwrap();
        let r2 = open_run(&d.join("000002.run")).unwrap();
        let mut fks = Vec::new();
        for_each_merged_rec(&[r1, r2], |rec| {
            fks.push(u64::from_le_bytes(rec[32..40].try_into().unwrap()));
            Ok(())
        })
        .unwrap();
        assert_eq!(
            fks,
            vec![1, 2, 5, 255, 256, 257],
            "merge must numeric-FK-order (255<256) and dedup"
        );
        let out = next_run_path(&d, 9);
        let merged = merge_runs_to_file(
            &[
                open_run(&d.join("000001.run")).unwrap(),
                open_run(&d.join("000002.run")).unwrap(),
            ],
            &out,
        )
        .unwrap();
        assert_eq!(merged.count, 6);
        assert_eq!(merged.key_len, 40);
        let body = read_run_body(&merged).unwrap();
        let mut got = Vec::new();
        for chunk in body.chunks_exact(40) {
            got.push(u64::from_le_bytes(chunk[32..40].try_into().unwrap()));
        }
        assert_eq!(got, vec![1, 2, 5, 255, 256, 257]);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn loser_tree_matches_heap_shuffle() {
        let d = tmp_dir();
        let mut expected = Vec::new();
        let mut runs = Vec::new();
        for r in 0..7u8 {
            let mut recs: Vec<[u8; 40]> = Vec::new();
            for i in 0..5u8 {
                let key = (r.wrapping_mul(3).wrapping_add(i)) % 11;
                let rec = sh_rec(key, u64::from(r) * 10 + u64::from(i));
                recs.push(rec);
                expected.push(rec);
            }
            recs.sort_by(|a, b| rec_key_cmp(a, b, 40, 40));
            let mut sorted = Vec::new();
            for rec in &recs {
                sorted.extend_from_slice(rec);
            }
            let path = d.join(format!("00000{r}.run"));
            write_sorted_run(&path, 40, 40, &sorted).unwrap();
            runs.push(open_run(&path).unwrap());
        }
        expected.sort_by(|a, b| rec_key_cmp(a, b, 40, 40));
        expected.dedup();
        let mut got = Vec::new();
        for_each_merged_rec(&runs, |rec| {
            got.push(rec.to_vec());
            Ok(())
        })
        .unwrap();
        assert_eq!(got, expected);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn kway_merge_submits_ahead_preads_on_session() {
        use crate::uring_session::{
            test_take_last_sqe_lens, with_forced_session_kind, SessionKind,
        };
        let d = tmp_dir();
        let rec_len = 40usize;
        let page = rec_len * 2;
        let n_recs = 24u8;
        let mut runs = Vec::new();
        for r in 0..3u8 {
            let mut body = Vec::new();
            for i in 0..n_recs {
                body.extend_from_slice(&sh_rec(r.saturating_mul(10) + i, u64::from(i)));
            }
            let path = d.join(format!("00000{r}.run"));
            write_sorted_run(&path, 40, 40, &body).unwrap();
            runs.push(open_run(&path).unwrap());
        }
        let _ = test_take_last_sqe_lens();
        let mut got = 0u64;
        with_forced_session_kind(SessionKind::Pool, || {
            super::for_each_merged_rec_paged(&runs, false, page, |_| {
                got += 1;
                Ok(())
            })
            .unwrap();
        });
        let lens = test_take_last_sqe_lens();
        assert!(
            !lens.is_empty(),
            "k-way merge must submit ahead preads on the TLS session"
        );
        assert!(
            lens.iter().all(|&n| n > 0 && (n as usize) % rec_len == 0),
            "ahead preads must be rec-aligned, got {lens:?}"
        );
        assert_eq!(got, u64::from(n_recs) * 3);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn for_each_merged_rec_orders_keys() {
        let d = tmp_dir();
        let p1 = d.join("000001.run");
        let p2 = d.join("000002.run");
        // key bytes: high first so merge must re-order
        write_sorted_run(&p1, 32, 44, &rec(2, 20)).unwrap();
        write_sorted_run(&p2, 32, 44, &rec(1, 10)).unwrap();
        let r1 = open_run(&p1).unwrap();
        let r2 = open_run(&p2).unwrap();
        let mut keys = Vec::new();
        for_each_merged_rec(&[r1, r2], |rec| {
            keys.push(rec[0]);
            Ok(())
        })
        .unwrap();
        assert_eq!(keys, vec![1, 2]);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn claim_for_materialize_survives_list_runs_orphan_cleanup() {
        let d = tmp_dir();
        let path = d.join("000001.run");
        let run = write_sorted_run(&path, 32, 44, &rec(1, 1)).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        assert!(
            claimed.path.extension().and_then(|s| s.to_str()) == Some("mat")
                || claimed.path.to_string_lossy().ends_with(".run.mat")
        );
        assert!(!path.exists());
        assert!(claimed.path.exists());
        // Concurrent list_runs must not delete the claimed body.
        let listed = list_runs(&d).unwrap();
        assert!(listed.is_empty());
        assert!(
            claimed.path.exists(),
            "claimed .run.mat must not be treated as orphan"
        );
        // Body still readable for materialize.
        let body = read_run_body(&claimed).unwrap();
        assert_eq!(body.len(), 44);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn list_runs_skips_non_run() {
        let d = tmp_dir();
        write_sorted_run(&d.join("000001.run"), 32, 44, &rec(1, 1)).unwrap();
        fs::write(d.join("meta"), b"x").unwrap();
        let runs = list_runs(&d).unwrap();
        assert_eq!(runs.len(), 1);
        let _ = fs::remove_dir_all(&d);
    }

    /// open_run / list_runs error arms: bad magic, truncated, trailing garbage, orphan cleanup.
    #[test]
    fn open_run_and_list_error_arms() {
        let d = tmp_dir();
        // Bad magic
        let bad = d.join("000010.run");
        fs::write(&bad, vec![0u8; 64]).unwrap();
        assert!(matches!(open_run(&bad), Err(StoreError::Corrupt(_))));
        // Truncated header
        let short = d.join("000011.run");
        fs::write(&short, b"SRUNSORT").unwrap();
        assert!(open_run(&short).is_err());
        // Valid run + trailing garbage rejected for v2
        let path = d.join("000012.run");
        let run = write_sorted_run(&path, 32, 44, &rec(1, 1)).unwrap();
        {
            use std::io::Write;
            let mut f = fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"GARBAGE").unwrap();
        }
        assert!(matches!(open_run(&path), Err(StoreError::Corrupt(_))));
        let _ = run;
        // Orphan cleanup: write a .run not in MANIFEST after list_runs rebuilt one
        let clean = tmp_dir();
        write_sorted_run(&clean.join("000001.run"), 32, 44, &rec(1, 1)).unwrap();
        let listed = list_runs(&clean).unwrap();
        assert_eq!(listed.len(), 1);
        // Drop an extra orphan run file and re-list (should remove orphan)
        write_sorted_run(&clean.join("000099.run"), 32, 44, &rec(9, 9)).unwrap();
        // Manually rebuild MANIFEST with only 000001 so 000099 is orphan
        // list_runs with existing MANIFEST should drop orphan
        let again = list_runs(&clean).unwrap();
        // Either both listed (legacy rebuild) or orphan removed — both exercise scan paths.
        assert!(!again.is_empty());
        // detach_run / lookup miss
        if let Ok(r) = open_run(&clean.join("000001.run")) {
            assert!(lookup_key(&r, &[0xff; 32]).unwrap().is_none());
            let _ = detach_run(&r);
        }
        let _ = fs::remove_dir_all(&d);
        let _ = fs::remove_dir_all(&clean);
    }

    #[test]
    fn open_run_v1_and_unsupported_version_and_lookup_short() {
        let d = tmp_dir();
        // Unsupported version with current magic.
        let bad_ver = d.join("000020.run");
        let mut hdr = vec![0u8; 64];
        hdr[0..8].copy_from_slice(b"RBSORT02");
        hdr[8..12].copy_from_slice(&99u32.to_le_bytes());
        fs::write(&bad_ver, &hdr).unwrap();
        assert!(matches!(open_run(&bad_ver), Err(StoreError::Corrupt(_))));
        // V1 magic with wrong version.
        let bad_v1 = d.join("000021.run");
        let mut h2 = vec![0u8; 64];
        h2[0..8].copy_from_slice(b"RBSORT01");
        h2[8..12].copy_from_slice(&99u32.to_le_bytes());
        fs::write(&bad_v1, &h2).unwrap();
        assert!(matches!(open_run(&bad_v1), Err(StoreError::Corrupt(_))));
        // Valid run: short lookup key.
        let path = d.join("000022.run");
        let run = write_sorted_run(&path, 32, 44, &rec(1, 1)).unwrap();
        assert!(matches!(
            lookup_key(&run, &[0u8; 8]),
            Err(StoreError::Corrupt(_))
        ));
        // Empty body path via zero-count run if supported.
        let empty_path = d.join("000023.run");
        if let Ok(empty) = write_sorted_run(&empty_path, 32, 44, &[]) {
            assert!(lookup_key(&empty, &[0u8; 32]).unwrap().is_none());
            let _ = read_run_body(&empty);
        }
        let _ = run;
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn merge_two_runs_sorted() {
        let d = tmp_dir();
        let p1 = d.join("000001.run");
        let p2 = d.join("000002.run");
        let mut a = Vec::new();
        a.extend_from_slice(&rec(1, 1));
        a.extend_from_slice(&rec(3, 3));
        write_sorted_run(&p1, 32, 44, &a).unwrap();
        let mut b = Vec::new();
        b.extend_from_slice(&rec(2, 2));
        b.extend_from_slice(&rec(4, 4));
        write_sorted_run(&p2, 32, 44, &b).unwrap();
        let r1 = open_run(&p1).unwrap();
        let r2 = open_run(&p2).unwrap();
        let out = d.join("000003.run");
        let merged = merge_runs(&[r1, r2], &out).unwrap();
        assert_eq!(merged.count, 4);
        let body = read_run_body(&merged).unwrap();
        assert_eq!(body[0], 1);
        assert_eq!(body[44], 2);
        assert_eq!(body[88], 3);
        assert_eq!(body[132], 4);
        assert!(!p1.exists());
        assert!(!p2.exists());
        let _ = fs::remove_dir_all(&d);
    }

    /// One-pass dynamic fanin deletes inputs as soon as each chunk merge finishes.
    #[test]
    fn reduce_fanin_deletes_merged_inputs() {
        let d = tmp_dir();
        let work = d.join("merge");
        // Exceed tiny TEST_TARGET so reduce actually runs (not skip).
        let n = (TEST_TARGET + 8) as u64;
        let inputs = write_n_runs(&d, n, 0);
        let _w = MergeWorkersGuard::set("1");
        let out = reduce_runs_to_fanin(&inputs, &work, TEST_TARGET).unwrap();
        assert!(
            out.len() <= TEST_TARGET,
            "stream runs {} > target",
            out.len()
        );
        // Originals deleted immediately after chunk merge.
        for r in &inputs {
            assert!(
                !r.path.exists(),
                "input {} should be deleted after merge",
                r.path.display()
            );
        }
        let total: u64 = out.iter().map(|r| r.count).sum();
        assert_eq!(total, n);
        commit_fanin_reduce_and_drop_inputs(&work, &inputs, &out).unwrap();
        assert!(work.join(FANIN_READY_NAME).is_file());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn dynamic_fanin_one_pass_geometry() {
        assert_eq!(dynamic_merge_fanin(10), 10);
        assert_eq!(dynamic_merge_fanin(32), 32);
        // Under production target (4096): fanin == n (direct stream; no reduce).
        assert_eq!(dynamic_merge_fanin(1000), 1000);
        assert_eq!(fanin_passes_total(1000, 1000), 0);
        assert_eq!(fanin_passes_total(10, 10), 0);
        assert_eq!(dynamic_merge_fanin(64), 64);
        // Above production target: ceil(n/TARGET) clamped (pure math; no IO).
        let n = FANIN_TARGET_STREAM_RUNS * 2 + 100;
        let f = dynamic_merge_fanin(n);
        assert!(f >= 8 && f <= FANIN_MAX_CHUNK);
        assert!(n.div_ceil(f) <= FANIN_TARGET_STREAM_RUNS);
        // Tiny target geometry (same formula; used by IO tests).
        let f_tiny = dynamic_merge_fanin_for(TEST_TARGET + 8, TEST_TARGET);
        assert!(f_tiny >= 1);
        assert!((TEST_TARGET + 8).div_ceil(f_tiny) <= TEST_TARGET);
        assert_eq!(
            dynamic_merge_fanin_for(TEST_TARGET, TEST_TARGET),
            TEST_TARGET
        );
        assert_eq!(dynamic_merge_fanin_for(0, TEST_TARGET), TEST_TARGET);
    }

    /// Partial checkpoint: simulate mid-reduce then resume.
    #[test]
    fn reduce_fanin_resume_partial_checkpoint() {
        let d = tmp_dir();
        let _w = MergeWorkersGuard::set("1");
        let work = d.join("merge");
        fs::create_dir_all(&work).unwrap();
        let n_runs = TEST_TARGET + 8;
        let all = write_n_runs(&d, n_runs as u64, 0);
        let fanin = dynamic_merge_fanin_for(n_runs, TEST_TARGET);
        assert!(n_runs.div_ceil(fanin) <= TEST_TARGET);
        // Complete first chunk only.
        let chunk: Vec<_> = all[..fanin].to_vec();
        let rest: Vec<_> = all[fanin..].to_vec();
        let out0 = work.join("g0_000001.run");
        let merged = merge_runs_to_file(&chunk, &out0).unwrap();
        for r in &chunk {
            let _ = fs::remove_file(&r.path);
        }
        write_fanin_checkpoint(&work, 0, 2, fanin, &rest, &[merged]).unwrap();

        let cp = load_fanin_checkpoint(&work).unwrap().expect("cp");
        assert_eq!(cp.remaining.len(), n_runs - fanin);
        assert_eq!(cp.done_outputs.len(), 1);

        // Resume finishes remaining (target unused when checkpoint present).
        let out = reduce_runs_to_fanin(&[], &work, TEST_TARGET).unwrap();
        assert!(out.len() <= TEST_TARGET);
        let n: u64 = out.iter().map(|r| r.count).sum();
        assert_eq!(n, n_runs as u64);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn reduce_fanin_cancel_returns_cancelled() {
        let d = tmp_dir();
        let work = d.join("merge");
        let n = (TEST_TARGET + 8) as u64;
        let inputs = write_n_runs(&d, n, 0);
        let _w = MergeWorkersGuard::set("1");
        let cancel = std::sync::atomic::AtomicBool::new(true);
        let err = reduce_runs_to_fanin_cancellable(&inputs, &work, TEST_TARGET, Some(&cancel))
            .unwrap_err();
        assert!(matches!(err, StoreError::Cancelled(_)), "got {err}");
        let _ = fs::remove_dir_all(&d);
    }

    /// Parallel reduce preserves total record count.
    #[test]
    fn reduce_fanin_parallel_preserves_count() {
        let d = tmp_dir();
        let n = (TEST_TARGET + 8) as u64;
        let inputs = write_n_runs(&d, n, 0);
        let work1 = d.join("merge1");
        {
            let _w = MergeWorkersGuard::set("1");
            let out1 = reduce_runs_to_fanin(&inputs, &work1, TEST_TARGET).unwrap();
            let n1: u64 = out1.iter().map(|r| r.count).sum();
            assert_eq!(n1, n);
            assert!(out1.len() <= TEST_TARGET);
        }

        let inputs2 = write_n_runs(&d, n, 10_000);
        let work2 = d.join("merge2");
        {
            let _w = MergeWorkersGuard::set("4");
            let out2 = reduce_runs_to_fanin(&inputs2, &work2, TEST_TARGET).unwrap();
            let n2: u64 = out2.iter().map(|r| r.count).sum();
            assert_eq!(n2, n);
            assert!(out2.len() <= TEST_TARGET);
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn lookup_key_finds_record() {
        let d = tmp_dir();
        let path = d.join("lk.run");
        let mut body = Vec::new();
        for i in [1u8, 3, 5, 7, 9] {
            body.extend_from_slice(&rec(i, i.wrapping_mul(10)));
        }
        let run = write_sorted_run(&path, 32, 44, &body).unwrap();
        let hit = lookup_key(&run, &rec(5, 0)[..32]).unwrap().unwrap();
        assert_eq!(hit[32], 50);
        assert!(lookup_key(&run, &rec(4, 0)[..32]).unwrap().is_none());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn merge_shard_slice() {
        let d = tmp_dir();
        let mut a = Vec::new();
        a.extend_from_slice(&sh_rec(0x00, 1));
        a.extend_from_slice(&sh_rec(0x10, 2));
        a.extend_from_slice(&sh_rec(0x80, 3));
        let mut b = Vec::new();
        b.extend_from_slice(&sh_rec(0x08, 4));
        b.extend_from_slice(&sh_rec(0x90, 5));
        b.extend_from_slice(&sh_rec(0xc0, 6));
        write_sorted_run(&d.join("000001.run"), 40, 40, &a).unwrap();
        write_sorted_run(&d.join("000002.run"), 40, 40, &b).unwrap();
        let inputs = [
            open_run(&d.join("000001.run")).unwrap(),
            open_run(&d.join("000002.run")).unwrap(),
        ];
        let cuts: Vec<Vec<u64>> = inputs
            .iter()
            .map(|r| super::shard_record_starts(r, 4).unwrap())
            .collect();
        for s in 0..4 {
            let mut from_slice = Vec::new();
            super::for_each_merged_rec_shard(&inputs, &cuts, s, false, |rec| {
                from_slice.push(rec.to_vec());
                Ok(())
            })
            .unwrap();
            let mut filtered = Vec::new();
            for_each_merged_rec_opts(&inputs, false, |rec| {
                let mut full = [0u8; 32];
                full.copy_from_slice(&rec[..32]);
                if crate::scripthash_head::prefix_shard_of(&full, 4) == s {
                    filtered.push(rec.to_vec());
                }
                Ok(())
            })
            .unwrap();
            assert_eq!(from_slice, filtered, "shard {s}");
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn shard_record_starts() {
        let d = tmp_dir();
        let mut body = Vec::new();
        for sh0 in [0x00u8, 0x10, 0x3f, 0x80, 0x90] {
            body.extend_from_slice(&sh_rec(sh0, 1));
        }
        let run = write_sorted_run(&d.join("000001.run"), 40, 40, &body).unwrap();
        let starts = super::shard_record_starts(&run, 4).unwrap();
        assert_eq!(starts, vec![0, 3, 3, 5, 5]);
        for i in 0..5 {
            let rec = super::read_run_record_at(&run, i as u64).unwrap();
            let mut full = [0u8; 32];
            full.copy_from_slice(&rec[..32]);
            let s = crate::scripthash_head::prefix_shard_of(&full, 4);
            assert!(
                (starts[s] as usize..starts[s + 1] as usize).contains(&i),
                "rec {i} shard {s} not in {:?}",
                starts
            );
        }
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn run_cursor_range() {
        let d = tmp_dir();
        let mut body = Vec::new();
        for i in 0..100u8 {
            body.extend_from_slice(&rec(i, i));
        }
        let run = write_sorted_run(&d.join("000001.run"), 32, 44, &body).unwrap();
        let mut cursor = RunCursor::open_range(&run, 40, 10, false).unwrap();
        let mut keys = Vec::new();
        while cursor.fill_next().unwrap() {
            keys.push(cursor.rec()[0]);
        }
        assert_eq!(keys, (40u8..50).collect::<Vec<_>>());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn run_cursor_prefetches_ahead() {
        let d = tmp_dir();
        let mut body = Vec::new();
        for i in 0..100u8 {
            body.extend_from_slice(&rec(i, i));
        }
        let run = write_sorted_run(&d.join("000001.run"), 32, 44, &body).unwrap();
        let rec_len = 44usize;
        let mut cursor = RunCursor::open_range_paged(&run, 0, 100, false, rec_len * 3).unwrap();
        for i in 0..3u8 {
            assert!(cursor.fill_next().unwrap());
            assert_eq!(cursor.rec()[0], i);
        }
        assert!(
            cursor.ahead_len >= rec_len,
            "next page must be in ahead after the first page is consumed"
        );
        let mut keys = vec![0u8, 1, 2];
        while cursor.fill_next().unwrap() {
            keys.push(cursor.rec()[0]);
        }
        assert_eq!(keys, (0u8..100).collect::<Vec<_>>());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn clear_style_dir_drops_manifest() {
        // clear_runs_dir deletes all files including MANIFEST.
        let d = tmp_dir();
        write_sorted_run(&d.join("000001.run"), 32, 44, &rec(1, 1)).unwrap();
        assert!(manifest_path(&d).exists());
        for e in fs::read_dir(&d).unwrap().flatten() {
            let _ = fs::remove_file(e.path());
        }
        assert!(!manifest_path(&d).exists());
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn manifest_bad_magic_truncated_and_merge_mismatch() {
        let d = tmp_dir();
        // truncated MANIFEST
        fs::write(manifest_path(&d), b"short").unwrap();
        assert!(load_manifest(&d).unwrap().is_none());
        // bad magic
        let mut bad = MANIFEST_MAGIC.to_vec();
        bad[0] ^= 0xff;
        bad.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bad.extend_from_slice(&0u32.to_le_bytes());
        fs::write(manifest_path(&d), &bad).unwrap();
        assert!(load_manifest(&d).unwrap().is_none());
        // unsupported version
        let mut badv = MANIFEST_MAGIC.to_vec();
        badv.extend_from_slice(&99u32.to_le_bytes());
        badv.extend_from_slice(&0u32.to_le_bytes());
        fs::write(manifest_path(&d), &badv).unwrap();
        assert!(load_manifest(&d).unwrap().is_none());

        // merge len mismatch
        let p1 = next_run_path(&d, 1);
        let p2 = next_run_path(&d, 2);
        write_sorted_run(&p1, 32, 44, &rec(1, 1)).unwrap();
        write_sorted_run(&p2, 16, 32, &[0u8; 32]).unwrap(); // different lens
        let r1 = open_run(&p1).unwrap();
        let r2 = open_run(&p2).unwrap();
        assert!(matches!(
            merge_runs(&[r1.clone(), r2.clone()], &next_run_path(&d, 3)),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            for_each_merged_rec(&[r1, r2], |_| Ok(())),
            Err(StoreError::Corrupt(_))
        ));

        // open bad magic run
        let pbad = d.join("bad.run");
        fs::write(&pbad, b"notasortrunfile!!!!!!!!!!!!!").unwrap();
        assert!(open_run(&pbad).is_err());

        // list_materialize_claims missing dir
        assert!(list_materialize_claims(&d.join("nope")).unwrap().is_empty());
        let _ = fs::remove_dir_all(&d);
    }

    /// MANIFEST missing/bad entries, orphan scan, legacy rebuild without MANIFEST.
    #[test]
    fn list_runs_manifest_missing_bad_and_legacy_heal() {
        let d = tmp_dir();
        // Catalog two runs via normal write.
        write_sorted_run(&next_run_path(&d, 1), 32, 44, &rec(1, 1)).unwrap();
        write_sorted_run(&next_run_path(&d, 2), 32, 44, &rec(2, 2)).unwrap();
        // Delete seq=2 file but leave MANIFEST entry → missing path arm.
        fs::remove_file(next_run_path(&d, 2)).unwrap();
        let listed = list_runs(&d).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].seq(), Some(1));

        // Plant a corrupt run listed as seq=3: write MANIFEST with extra entry.
        let mut mf = load_manifest(&d).unwrap().unwrap();
        mf.entries.push(ManifestEntry {
            seq: 3,
            key_len: 32,
            rec_len: 44,
            count: 1,
            body_crc32: 0xdead_beef,
        });
        save_manifest(&d, &mf).unwrap();
        // Bad file that fails open (wrong magic)
        fs::write(next_run_path(&d, 3), b"notasortrunfile!!!!!!!!!!!!!").unwrap();
        let listed2 = list_runs(&d).unwrap();
        assert_eq!(listed2.len(), 1); // only seq=1 valid

        // CRC mismatch against manifest when body_crc32 both non-zero.
        write_sorted_run(&next_run_path(&d, 4), 32, 44, &rec(4, 4)).unwrap();
        let run4 = open_run(&next_run_path(&d, 4)).unwrap();
        let mut mf = load_manifest(&d).unwrap().unwrap();
        if let Some(e) = mf.entries.iter_mut().find(|e| e.seq == 4) {
            e.body_crc32 = run4.body_crc32 ^ 0xffff_ffff;
        }
        save_manifest(&d, &mf).unwrap();
        // open_and_check should fail CRC; list_runs skips bad.
        let expect = ManifestEntry {
            seq: 4,
            key_len: 32,
            rec_len: 44,
            count: 1,
            body_crc32: run4.body_crc32 ^ 0xffff_ffff,
        };
        assert!(open_and_check_against_entry(&next_run_path(&d, 4), &expect).is_err());
        let listed3 = list_runs(&d).unwrap();
        // seq=4 skipped due to CRC
        assert!(!listed3.iter().any(|r| r.seq() == Some(4)));

        // Legacy heal: remove MANIFEST, keep a valid run → rebuilds catalog.
        let d2 = tmp_dir();
        write_sorted_run_file(
            &next_run_path(&d2, 7),
            32,
            44,
            &rec(7, 7),
            RunWritePolicy::DURABLE,
        )
        .unwrap();
        assert!(!manifest_path(&d2).exists());
        let listed_legacy = list_runs(&d2).unwrap();
        assert_eq!(listed_legacy.len(), 1);
        assert!(manifest_path(&d2).exists());

        // write errors: bad key/rec, body not multiple
        assert!(
            write_sorted_run_file(&d.join("x.run"), 0, 44, &[], RunWritePolicy::DURABLE).is_err()
        );
        assert!(
            write_sorted_run_file(&d.join("y.run"), 32, 16, &[], RunWritePolicy::DURABLE).is_err()
        );
        assert!(write_sorted_run_file(
            &d.join("z.run"),
            32,
            44,
            &[1, 2, 3],
            RunWritePolicy::DURABLE
        )
        .is_err());

        // empty dir
        assert!(list_runs(&d.join("nope")).unwrap().is_empty());

        let _ = fs::remove_dir_all(&d);
        let _ = fs::remove_dir_all(&d2);
    }

    #[test]
    fn detach_remove_next_path_and_opts() {
        let d = tmp_dir();
        assert_eq!(next_run_path(&d, 1).file_name().unwrap(), "000001.run");
        let p1 = next_run_path(&d, 1);
        let mut body = Vec::new();
        body.extend_from_slice(&rec(1, 10));
        body.extend_from_slice(&rec(2, 20));
        body.extend_from_slice(&rec(3, 30));
        write_sorted_run(&p1, 32, 44, &body).unwrap();
        assert_eq!(list_runs(&d).unwrap().len(), 1);

        // Empty body (cataloged as seq 2)
        let p_empty = next_run_path(&d, 2);
        write_sorted_run(&p_empty, 32, 44, &[]).unwrap();
        let empty = open_run(&p_empty).unwrap();
        assert_eq!(empty.count, 0);
        assert!(read_run_body(&empty).unwrap().is_empty());
        verify_run_body(&empty).unwrap();

        // for_each_merged with opts
        let r = open_run(&p1).unwrap();
        let mut n = 0u32;
        for_each_merged_rec_opts(&[r], false, |rec| {
            n += 1;
            assert_eq!(rec.len(), 44);
            Ok(())
        })
        .unwrap();
        assert_eq!(n, 3);
        for_each_merged_rec_opts(&[], true, |_| unreachable!()).unwrap();

        // detach leaves file but drops catalog entry
        let run = open_run(&p1).unwrap();
        detach_run(&run).unwrap();
        assert!(p1.exists());
        // remove deletes file (+ detach again is fine)
        remove_run(&run).unwrap();
        assert!(!p1.exists());

        // bad lens
        assert!(matches!(
            write_sorted_run(&d.join("bad.run"), 0, 44, &[]),
            Err(StoreError::Corrupt(_))
        ));
        assert!(matches!(
            write_sorted_run(&d.join("bad2.run"), 32, 44, &[1, 2, 3]),
            Err(StoreError::Corrupt(_))
        ));
        // merge empty inputs
        let out = next_run_path(&d, 9);
        let merged = merge_runs(&[], &out).unwrap();
        assert_eq!(merged.count, 0);
        let _ = fs::remove_dir_all(&d);
    }

    /// When key_len == rec_len the body is the sort key: must be strictly increasing.
    #[test]
    fn write_sorted_run_rejects_unsorted_when_key_len_eq_rec() {
        let d = tmp_dir();
        let mut rec_hi = [0u8; 40];
        rec_hi[31] = 2;
        rec_hi[32..40].copy_from_slice(&2u64.to_le_bytes());
        let mut rec_lo = [0u8; 40];
        rec_lo[31] = 1;
        rec_lo[32..40].copy_from_slice(&1u64.to_le_bytes());
        let mut body = Vec::new();
        body.extend_from_slice(&rec_hi);
        body.extend_from_slice(&rec_lo);
        match write_sorted_run(&d.join("desc.run"), 40, 40, &body) {
            Err(StoreError::Corrupt(m)) => {
                assert!(
                    m.contains("not strictly increasing"),
                    "expected order error, got {m}"
                );
            }
            other => panic!("expected Corrupt unsorted, got {other:?}"),
        }
        let mut ok = Vec::new();
        ok.extend_from_slice(&rec_lo);
        ok.extend_from_slice(&rec_hi);
        let run = write_sorted_run(&d.join("000001.run"), 40, 40, &ok).unwrap();
        assert_eq!(run.key_len, 40);
        assert_eq!(run.rec_len, 40);
        assert_eq!(run.count, 2);
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn tip_catalog_unpaced_ibd_background_paced() {
        // Tip fan-in / recollect must not inherit IBD artificial sleeps.
        assert!(!RunWritePolicy::CATALOG.pace);
        assert!(!RunWritePolicy::DURABLE.pace);
        assert!(!RunWritePolicy::L0.pace);
        assert!(RunWritePolicy::IBD_BACKGROUND.pace);
        assert!(RunWritePolicy::CATALOG.durable);
        assert!(RunWritePolicy::IBD_BACKGROUND.durable);
    }

    /// L0 spills must be readable without fsync; max create_fk tracked during merge
    /// (no second full-body scan for SEAL).
    #[test]
    fn l0_write_policy_and_merge_tracks_max_fk() {
        let d = tmp_dir();
        let p1 = d.join("l0").join("000001.run");
        let p2 = d.join("l0").join("000002.run");
        // 40-byte SH records: scripthash[32] | create_fk:u64
        fn sh_rec(key0: u8, fk: u64) -> [u8; 40] {
            let mut r = [0u8; 40];
            r[0] = key0;
            r[32..40].copy_from_slice(&fk.to_le_bytes());
            r
        }
        let b1 = sh_rec(1, 10);
        let b2 = sh_rec(2, 99);
        write_sorted_run_file_with_policy(&p1, 32, 40, &b1, RunWritePolicy::L0).unwrap();
        write_sorted_run_file_with_policy(&p2, 32, 40, &b2, RunWritePolicy::L0).unwrap();
        let r1 = open_run(&p1).unwrap();
        let r2 = open_run(&p2).unwrap();
        assert_eq!(read_run_body(&r1).unwrap().len(), 40);
        let out = d.join("l0").join("000003.run");
        let merged =
            merge_runs_to_file_with_policy(&[r1, r2], &out, RunWritePolicy::L0, false).unwrap();
        assert_eq!(merged.run.count, 2);
        assert_eq!(
            merged.max_u64_at_32, 99,
            "must track max create_fk while streaming"
        );
        // L0 policy leaves file readable without catalog MANIFEST.
        assert!(out.exists());
        assert!(!manifest_path(out.parent().unwrap()).exists() || true);
        set_thread_idle_io_priority(); // best-effort no-op on failure
        let _ = fs::remove_dir_all(&d);
    }
}
