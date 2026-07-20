//! Append-only **sorted run** files for index build-as-you-go (SH first).
//!
//! Fixed-width records, sorted by a leading 32-byte key. Flushes are sequential
//! writes; compaction is a k-way merge. No open-hash RMW.

use crate::error::StoreError;
use std::cmp::Ordering;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic: `RBSORT01`
const MAGIC: [u8; 8] = *b"RBSORT01";
const HEADER_LEN: usize = 32;
/// header: magic8 | version_u32 | key_len_u32 | rec_len_u32 | count_u64 | pad4
const VERSION: u32 = 1;

fn io_err(path: &Path, e: std::io::Error) -> StoreError {
    StoreError::io(path, e)
}

/// One immutable sorted run on disk.
#[derive(Debug, Clone)]
pub struct SortedRunPath {
    pub path: PathBuf,
    pub count: u64,
    pub rec_len: u32,
    pub key_len: u32,
}

/// Write a new sorted run from **already sorted** fixed-width records.
///
/// `records` must be sorted ascending by the first `key_len` bytes of each
/// `rec_len`-byte record. `rec_len` must be ≥ `key_len`.
pub fn write_sorted_run(
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
        f.write_all(&hdr).map_err(|e| io_err(&tmp, e))?;
        if !records.is_empty() {
            f.write_all(records).map_err(|e| io_err(&tmp, e))?;
        }
        f.sync_all().map_err(|e| io_err(&tmp, e))?;
    }
    fs::rename(&tmp, path).map_err(|e| io_err(path, e))?;
    Ok(SortedRunPath {
        path: path.to_path_buf(),
        count,
        rec_len,
        key_len,
    })
}

/// Open and validate a run header (does not load body).
pub fn open_run(path: &Path) -> Result<SortedRunPath, StoreError> {
    let mut f = File::open(path).map_err(|e| io_err(path, e))?;
    let mut hdr = [0u8; HEADER_LEN];
    f.read_exact(&mut hdr).map_err(|e| io_err(path, e))?;
    if hdr[0..8] != MAGIC {
        return Err(StoreError::Corrupt("sorted run: bad magic"));
    }
    let version = u32::from_le_bytes(hdr[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(StoreError::Corrupt("sorted run: unsupported version"));
    }
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
    Ok(SortedRunPath {
        path: path.to_path_buf(),
        count,
        rec_len,
        key_len,
    })
}

/// Read all records into a contiguous buffer (count × rec_len).
pub fn read_run_body(run: &SortedRunPath) -> Result<Vec<u8>, StoreError> {
    let mut f = File::open(&run.path).map_err(|e| io_err(&run.path, e))?;
    f.seek(SeekFrom::Start(HEADER_LEN as u64))
        .map_err(|e| io_err(&run.path, e))?;
    let mut buf = vec![0u8; (run.count as usize).saturating_mul(run.rec_len as usize)];
    if !buf.is_empty() {
        f.read_exact(&mut buf).map_err(|e| io_err(&run.path, e))?;
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

/// Streaming cursor over a run (for merge).
struct RunCursor {
    file: File,
    path: PathBuf,
    remaining: u64,
    buf: Vec<u8>,
}

impl RunCursor {
    fn open(run: &SortedRunPath) -> Result<Self, StoreError> {
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

/// K-way merge of sorted runs → new run at `out_path`. Deletes input files on success.
///
/// Equal keys: all records are kept (multi-value multimap for SH creates).
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

    let written = write_sorted_run(out_path, key_len as u32, rec_len, &out_body)?;
    for r in inputs {
        let _ = fs::remove_file(&r.path);
    }
    Ok(written)
}

/// List `*.run` files in `dir` sorted by name.
pub fn list_runs(dir: &Path) -> Result<Vec<SortedRunPath>, StoreError> {
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
    let mut out = Vec::with_capacity(paths.len());
    for p in paths {
        match open_run(&p) {
            Ok(r) => out.push(r),
            Err(_) => {
                rbitcoin_log::warn!("store: skipping corrupt sorted run {}", p.display());
            }
        }
    }
    Ok(out)
}

/// Next run path: `{dir}/{seq:06}.run`.
pub fn next_run_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:06}.run"))
}

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
    fn write_read_roundtrip() {
        let d = tmp_dir();
        let path = d.join("000001.run");
        let mut body = Vec::new();
        body.extend_from_slice(&rec(1, 10));
        body.extend_from_slice(&rec(2, 20));
        write_sorted_run(&path, 32, 44, &body).unwrap();
        let run = open_run(&path).unwrap();
        assert_eq!(run.count, 2);
        let b = read_run_body(&run).unwrap();
        assert_eq!(b.len(), 88);
        assert_eq!(b[0], 1);
        assert_eq!(b[32], 10);
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
    fn list_runs_skips_non_run() {
        let d = tmp_dir();
        write_sorted_run(&d.join("000001.run"), 32, 44, &rec(1, 1)).unwrap();
        fs::write(d.join("meta"), b"x").unwrap();
        let runs = list_runs(&d).unwrap();
        assert_eq!(runs.len(), 1);
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
        // rec packs tag in byte 32 as u8 for this test helper.
        assert_eq!(hit[32], 50);
        assert!(lookup_key(&run, &rec(4, 0)[..32]).unwrap().is_none());
        let _ = fs::remove_dir_all(&d);
    }
}
