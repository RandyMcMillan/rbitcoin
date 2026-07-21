//! Append-only **sorted run** files for index build-as-you-go (SH / tx / point).
//!
//! Fixed-width records, sorted by a leading key. Integrity:
//! - Each run stores a **CRC-32** of the body in the header (format v2).
//! - A directory **`MANIFEST`** lists the authoritative set of runs (seq, lens,
//!   count, crc). Updated only via write-tmp → fsync → rename after a successful
//!   run write or merge, so a crash cannot leave a half-published set.
//! - [`list_runs`] trusts the manifest (not a raw directory scan) when present;
//!   missing listed files or CRC/header mismatch are reported; orphan `.run`
//!   files are ignored (and removed best-effort).

use crate::error::StoreError;
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

// ── CRC-32 (ISO-HDLC / Ethernet polynomial) ─────────────────────────────────

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

// ── Run path ────────────────────────────────────────────────────────────────

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

// ── Manifest ────────────────────────────────────────────────────────────────

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
        rbitcoin_log::warn!(
            "store: sorted-run MANIFEST truncated at {}",
            path.display()
        );
        return Ok(None);
    }
    if hdr[0..8] != MANIFEST_MAGIC {
        rbitcoin_log::warn!(
            "store: sorted-run MANIFEST bad magic at {}",
            path.display()
        );
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

// ── Write / open ────────────────────────────────────────────────────────────

/// Write a new sorted run from **already sorted** fixed-width records.
///
/// `records` must be sorted ascending by the first `key_len` bytes of each
/// `rec_len`-byte record. Updates the parent directory [`MANIFEST`] when the
/// file name is `{seq:06}.run`.
pub fn write_sorted_run(
    path: &Path,
    key_len: u32,
    rec_len: u32,
    records: &[u8],
) -> Result<SortedRunPath, StoreError> {
    let run = write_sorted_run_file(path, key_len, rec_len, records)?;
    if let Some(dir) = path.parent() {
        manifest_insert(dir, &run)?;
    }
    Ok(run)
}

/// Write run file only (no manifest). Used by merge for a single catalog commit.
fn write_sorted_run_file(
    path: &Path,
    key_len: u32,
    rec_len: u32,
    records: &[u8],
) -> Result<SortedRunPath, StoreError> {
    if key_len == 0 || rec_len < key_len {
        return Err(StoreError::Corrupt("sorted run: bad key/rec len"));
    }
    if records.len() % rec_len as usize != 0 {
        return Err(StoreError::Corrupt("sorted run: body not multiple of rec_len"));
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
            f.write_all(records).map_err(|e| io_err(&tmp, e))?;
        }
        f.sync_all().map_err(|e| io_err(&tmp, e))?;
    }
    fs::rename(&tmp, path).map_err(|e| io_err(path, e))?;
    if let Some(parent) = path.parent() {
        if let Ok(dirf) = File::open(parent) {
            let _ = dirf.sync_all();
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
                // Walk left to first equal (stable first).
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

// ── Merge ───────────────────────────────────────────────────────────────────

/// Streaming cursor over a run (for merge).
struct RunCursor {
    file: File,
    path: PathBuf,
    remaining: u64,
    buf: Vec<u8>,
}

impl RunCursor {
    fn open(run: &SortedRunPath) -> Result<Self, StoreError> {
        // Cheap integrity before streaming a multi-GB merge.
        verify_run_body(run)?;
        let mut file = File::open(&run.path).map_err(|e| io_err(&run.path, e))?;
        file.seek(SeekFrom::Start(HEADER_LEN as u64))
            .map_err(|e| io_err(&run.path, e))?;
        Ok(Self {
            file,
            path: run.path.clone(),
            remaining: run.count,
            buf: vec![0u8; run.rec_len as usize],
        })
    }

    fn next_rec(&mut self) -> Result<Option<Vec<u8>>, StoreError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        self.file
            .read_exact(&mut self.buf)
            .map_err(|e| io_err(&self.path, e))?;
        self.remaining -= 1;
        Ok(Some(self.buf.clone()))
    }
}

struct MergeHead {
    cursor: RunCursor,
    rec: Vec<u8>,
    idx: usize,
}

fn head_less(a: &MergeHead, b: &MergeHead, key_len: usize) -> bool {
    match a.rec[..key_len].cmp(&b.rec[..key_len]) {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => a.idx < b.idx,
    }
}

fn sift_down(heap: &mut [MergeHead], mut i: usize, key_len: usize) {
    let n = heap.len();
    loop {
        let l = 2 * i + 1;
        let r = 2 * i + 2;
        let mut smallest = i;
        if l < n && head_less(&heap[l], &heap[smallest], key_len) {
            smallest = l;
        }
        if r < n && head_less(&heap[r], &heap[smallest], key_len) {
            smallest = r;
        }
        if smallest == i {
            break;
        }
        heap.swap(i, smallest);
        i = smallest;
    }
}

fn sift_up(heap: &mut [MergeHead], mut i: usize, key_len: usize) {
    while i > 0 {
        let p = (i - 1) / 2;
        if head_less(&heap[i], &heap[p], key_len) {
            heap.swap(i, p);
            i = p;
        } else {
            break;
        }
    }
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
pub fn claim_run_for_materialize(run: &SortedRunPath) -> Result<SortedRunPath, StoreError> {
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

/// K-way merge of sorted runs → new run at `out_path`. Deletes input files on success.
///
/// Equal keys: all records are kept (multi-value multimap for SH creates).
/// Single atomic MANIFEST update: drop inputs, add output.
pub fn merge_runs(inputs: &[SortedRunPath], out_path: &Path) -> Result<SortedRunPath, StoreError> {
    if inputs.is_empty() {
        return write_sorted_run(out_path, 32, 44, &[]);
    }
    let key_len = inputs[0].key_len as usize;
    let rec_len = inputs[0].rec_len;
    for r in inputs {
        if r.key_len as usize != key_len || r.rec_len != rec_len {
            return Err(StoreError::Corrupt("sorted run: merge len mismatch"));
        }
    }

    let mut heap: Vec<MergeHead> = Vec::new();
    for (idx, run) in inputs.iter().enumerate() {
        let mut cursor = RunCursor::open(run)?;
        if let Some(rec) = cursor.next_rec()? {
            heap.push(MergeHead { cursor, rec, idx });
        }
    }
    for i in (0..heap.len()).rev() {
        sift_down(&mut heap, i, key_len);
    }

    let mut out_body = Vec::new();
    let total: u64 = inputs.iter().map(|r| r.count).sum();
    out_body.reserve((total as usize).saturating_mul(rec_len as usize));

    while !heap.is_empty() {
        let mut min = heap.swap_remove(0);
        if !heap.is_empty() {
            sift_down(&mut heap, 0, key_len);
        }
        out_body.extend_from_slice(&min.rec);
        if let Some(next) = min.cursor.next_rec()? {
            min.rec = next;
            heap.push(min);
            let last = heap.len() - 1;
            sift_up(&mut heap, last, key_len);
        }
    }

    // 1) Write new run file (not yet in MANIFEST).
    // 2) Atomic MANIFEST: drop inputs, add output — crash after this still has
    //    a consistent catalog; leftover input files become orphans and are
    //    removed on the next list_runs.
    // 3) Delete input files.
    let written = write_sorted_run_file(out_path, key_len as u32, rec_len, &out_body)?;
    let remove_seqs: Vec<u64> = inputs.iter().filter_map(|r| r.seq()).collect();
    if let Some(dir) = out_path.parent() {
        manifest_merge_commit(dir, &remove_seqs, &written)?;
    }
    for r in inputs {
        let _ = fs::remove_file(&r.path);
    }
    Ok(written)
}

// ── List ────────────────────────────────────────────────────────────────────

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
    if run.count != expect.count
        || run.key_len != expect.key_len
        || run.rec_len != expect.rec_len
    {
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
                    rbitcoin_log::warn!(
                        "store: skipping bad sorted run {}: {e}",
                        path.display()
                    );
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
            Err(StoreError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                rbitcoin_log::warn!(
                    "store: skipping bad sorted run {}: {e}",
                    p.display()
                );
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

    #[test]
    fn crc32_known_vector() {
        // CRC-32/ISO-HDLC of "123456789"
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn write_read_roundtrip() {
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
        write_sorted_run_file(&orphan, 32, 44, &rec(9, 9)).unwrap();
        // MANIFEST still only has 000001.
        let runs = list_runs(&d).unwrap();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].seq(), Some(1));
        assert!(!orphan.exists(), "orphan should be removed");
        let _ = fs::remove_dir_all(&d);
    }

    #[test]
    fn claim_for_materialize_survives_list_runs_orphan_cleanup() {
        let d = tmp_dir();
        let path = d.join("000001.run");
        let run = write_sorted_run(&path, 32, 44, &rec(1, 1)).unwrap();
        let claimed = claim_run_for_materialize(&run).unwrap();
        assert!(claimed.path.extension().and_then(|s| s.to_str()) == Some("mat")
            || claimed.path.to_string_lossy().ends_with(".run.mat"));
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
}
