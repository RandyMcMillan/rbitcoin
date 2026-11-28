use rbitcoin_store::spend_delta::{
    idx_name, log_name, segment_name, CheckpointReport, Policy, SpendPageLog, SpentBody,
    SPEND_DELTA_COMMIT_NAME, SPEND_DELTA_DIR,
};
use std::path::PathBuf;

fn dir() -> PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let p = std::env::temp_dir().join(format!("rbitcoin-spend-delta-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// A [`dir`] removed when the test ends, on success or panic.
struct CleanDir(PathBuf);

impl CleanDir {
    /// The guard and its path.
    fn new() -> (Self, PathBuf) {
        let d = dir();
        (Self(d.clone()), d)
    }
}

impl Drop for CleanDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn delta_dir(d: &std::path::Path) -> PathBuf {
    d.join(SPEND_DELTA_DIR)
}

fn commit_path(d: &std::path::Path) -> PathBuf {
    delta_dir(d).join(SPEND_DELTA_COMMIT_NAME)
}

/// `spent.body` as a byte vector. `len` is the published end.
struct MemBody {
    bytes: Vec<u8>,
    syncs: usize,
    /// Fail every write, as a crash during the copy would stop it.
    fail_writes: bool,
    /// Page reads, each a random `pread` on a real body.
    reads: std::cell::Cell<usize>,
}

impl MemBody {
    fn new(len: usize) -> Self {
        Self {
            bytes: vec![0u8; len],
            syncs: 0,
            fail_writes: false,
            reads: std::cell::Cell::new(0),
        }
    }

    fn slot(&self, abs: u64) -> [u8; 8] {
        let at = abs as usize;
        self.bytes[at..at + 8].try_into().unwrap()
    }
}

impl SpentBody for MemBody {
    fn published_len(&self) -> u64 {
        self.bytes.len() as u64
    }

    fn read(&self, abs: u64, buf: &mut [u8]) -> Result<(), rbitcoin_store::error::StoreError> {
        let at = abs as usize;
        assert!(
            at + buf.len() <= self.bytes.len(),
            "a fold reads below the published end"
        );
        buf.copy_from_slice(&self.bytes[at..at + buf.len()]);
        self.reads.set(self.reads.get() + 1);
        Ok(())
    }

    fn write(&mut self, abs: u64, bytes: &[u8]) -> Result<(), rbitcoin_store::error::StoreError> {
        if self.fail_writes {
            return Err(rbitcoin_store::error::StoreError::Corrupt("injected crash"));
        }
        let at = abs as usize;
        assert!(
            at + bytes.len() <= self.bytes.len(),
            "a fold writes below the published end"
        );
        self.bytes[at..at + bytes.len()].copy_from_slice(bytes);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), rbitcoin_store::error::StoreError> {
        self.syncs += 1;
        Ok(())
    }
}

/// Open with a `spent.body` of `end` zero bytes: a body no fold wrote.
fn reload(
    d: &std::path::Path,
    end: u64,
) -> Result<SpendPageLog, rbitcoin_store::error::StoreError> {
    SpendPageLog::load_with(d, &mut MemBody::new(end as usize), Policy::DEFAULT)
}

fn commit(
    log: &SpendPageLog,
    tip: u32,
    _published: u64,
    keep: impl FnMut(&[u8; 8]) -> Result<bool, rbitcoin_store::error::StoreError>,
) {
    try_commit(log, tip, keep).unwrap();
}

fn try_commit(
    log: &SpendPageLog,
    tip: u32,
    keep: impl FnMut(&[u8; 8]) -> Result<bool, rbitcoin_store::error::StoreError>,
) -> Result<(), rbitcoin_store::error::StoreError> {
    folding_commit(log, tip, keep, &mut MemBody::new(0), None)
}

fn folding_commit(
    log: &SpendPageLog,
    tip: u32,
    keep: impl FnMut(&[u8; 8]) -> Result<bool, rbitcoin_store::error::StoreError>,
    body: &mut MemBody,
    durable: Option<u32>,
) -> Result<(), rbitcoin_store::error::StoreError> {
    reporting_commit(log, tip, keep, body, durable).map(|_| ())
}

fn reporting_commit(
    log: &SpendPageLog,
    tip: u32,
    keep: impl FnMut(&[u8; 8]) -> Result<bool, rbitcoin_store::error::StoreError>,
    body: &mut MemBody,
    durable: Option<u32>,
) -> Result<CheckpointReport, rbitcoin_store::error::StoreError> {
    log.checkpoint(|l| l.freeze_open(tip, Some(keep)), body, durable)
}

#[test]
fn a_fast_checkpoint_logs_one_info_summary_and_debug_detail() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, [9u8; 8])]).unwrap();
    rbitcoin_log::capture_logs(true);
    commit(&log, 1, 4096, |_| Ok(true));
    let logs = rbitcoin_log::take_logs();
    rbitcoin_log::capture_logs(false);

    let infos: Vec<_> = logs
        .iter()
        .filter(|(level, msg)| {
            *level == rbitcoin_log::Level::Info && msg.starts_with("store: spend delta gen=")
        })
        .collect();
    assert_eq!(infos.len(), 1, "{logs:?}");
    assert!(infos[0].1.contains("since_ms="), "{:?}", infos[0]);
    assert!(infos[0].1.contains("total_ms="), "{:?}", infos[0]);
    assert!(!infos[0].1.contains(" timing "), "{:?}", infos[0]);

    let details: Vec<_> = logs
        .iter()
        .filter(|(level, msg)| {
            *level == rbitcoin_log::Level::Debug
                && msg.starts_with("store: spend delta gen=")
                && msg.contains(" timing ")
        })
        .collect();
    assert_eq!(details.len(), 1, "{logs:?}");
}

#[test]
fn a_slow_checkpoint_promotes_its_detail_to_info() {
    let mut r = CheckpointReport::default();
    assert!(!rbitcoin_store::spend_delta::checkpoint_detail_is_info(&r));
    r.total_ms = rbitcoin_store::spend_delta::CHECKPOINT_SLOW_TOTAL_MS;
    assert!(rbitcoin_store::spend_delta::checkpoint_detail_is_info(&r));
    r.total_ms = 0;
    r.body_sync_ms = rbitcoin_store::spend_delta::CHECKPOINT_SLOW_SYNC_MS;
    assert!(rbitcoin_store::spend_delta::checkpoint_detail_is_info(&r));
}

#[test]
fn noted_slot_survives_commit_and_reload() {
    let d = dir();
    let pub_len = 4096u64;
    let meta = [9u8, 1, 2, 3, 4, 5, 6, 7];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, meta)]).unwrap();
        assert_eq!(log.slot(16).unwrap(), Some(meta));
        commit(&log, 1, pub_len, |_| Ok(true));
        assert_eq!(log.slot(16).unwrap(), Some(meta));
        assert_eq!(
            log.slots_at(&[16, 24]).unwrap(),
            vec![Some(meta), Some([0u8; 8])]
        );
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(meta));
    assert_eq!(
        log.slots_at(&[16, 24]).unwrap(),
        vec![Some(meta), Some([0u8; 8])]
    );
}

#[test]
fn explicit_zero_is_served_and_survives_reload() {
    let d = dir();
    let pub_len = 4096u64;
    let zero = [0u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, zero)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        assert_eq!(log.slot(16).unwrap(), Some(zero));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(zero));
}

#[test]
fn later_note_wins_across_commits() {
    let d = dir();
    let pub_len = 4096u64;
    let full = [3u8, 1, 2, 3, 4, 5, 6, 7];
    let zero = [0u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, full)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        log.note_slots(&[(16, zero)]).unwrap();
        commit(&log, 2, pub_len, |_| Ok(true));
        assert_eq!(log.slot(16).unwrap(), Some(zero));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(zero));
}

#[test]
fn rejected_new_slot_overrides_the_previous_delta() {
    let d = dir();
    let pub_len = 4096u64;
    let full = [4u8, 1, 2, 3, 4, 5, 6, 7];
    let zero = [0u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, full)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        log.note_slots(&[(16, full)]).unwrap();
        commit(&log, 2, pub_len, |_| Ok(false));
        assert_eq!(log.slot(16).unwrap(), Some(zero));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(zero));
}

#[test]
fn later_slot_on_the_same_page_keeps_the_earlier_override() {
    let d = dir();
    let pub_len = 4096u64;
    let a = [5u8, 1, 2, 3, 4, 5, 6, 7];
    let b = [6u8, 1, 2, 3, 4, 5, 6, 7];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        log.note_slots(&[(24, b)]).unwrap();
        commit(&log, 2, pub_len, |_| Ok(true));
        assert_eq!(log.slot(16).unwrap(), Some(a));
        assert_eq!(log.slot(24).unwrap(), Some(b));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(24).unwrap(), Some(b));
}

#[test]
fn page_directory_grows_across_pages() {
    let d = dir();
    let pub_len = 3 * 4096u64;
    let a = [7u8, 1, 2, 3, 4, 5, 6, 7];
    let b = [8u8, 1, 2, 3, 4, 5, 6, 7];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a), (4096 + 16, b)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        assert_eq!(log.slot(16).unwrap(), Some(a));
        assert_eq!(log.slot(4096 + 16).unwrap(), Some(b));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some(b));
}

/// A lost commit is the only name for every override. Records without it
/// refuse open instead of reading every tracked spend as unspent.
#[test]
fn records_without_a_commit_refuse_open() {
    let d = dir();
    let pub_len = 4096u64;
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, [1u8, 2, 3, 4, 5, 6, 7, 8])]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        std::fs::remove_file(commit_path(&d)).unwrap();
    }
    assert!(reload(&d, pub_len).is_err());
}

/// Save the commit, run `f`, then put the saved commit back: a kill after
/// the delta and directory syncs, before the commit rename.
fn crash_before_rename(d: &std::path::Path, f: impl FnOnce()) {
    let saved = std::fs::read(commit_path(d)).unwrap();
    f();
    std::fs::write(commit_path(d), saved).unwrap();
}

/// The next checkpoint after a crash takes the crashed attempt's generation
/// number. Open zeroed that attempt's slots, so none on an untouched page is
/// selected.
#[test]
fn uncommitted_generation_is_not_selected_after_reload() {
    let d = dir();
    let pub_len = 3 * 4096u64;
    let a = [1u8; 8];
    let b = [2u8; 8];
    let c = [3u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        crash_before_rename(&d, || {
            log.note_slots(&[(24, b), (4096 + 16, b)]).unwrap();
            commit(&log, 2, pub_len, |_| Ok(true));
        });
    }
    {
        let log = reload(&d, pub_len).unwrap();
        log.note_slots(&[(2 * 4096 + 16, c)]).unwrap();
        commit(&log, 2, pub_len, |_| Ok(true));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(24).unwrap(), Some([0u8; 8]));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some([0u8; 8]));
    assert_eq!(log.slot(2 * 4096 + 16).unwrap(), Some(c));
}

/// A checkpoint writes a page's entry into the slot the commit does not
/// name. Page 0 last written two generations ago keeps its entry when the
/// next write of page 0 crashes before the rename.
#[test]
fn rewrite_two_generations_later_keeps_the_committed_entry() {
    let d = dir();
    let pub_len = 2 * 4096u64;
    let a = [4u8; 8];
    let b = [5u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        log.note_slots(&[(4096 + 16, b)]).unwrap();
        commit(&log, 2, pub_len, |_| Ok(true));
        crash_before_rename(&d, || {
            log.note_slots(&[(16, b)]).unwrap();
            commit(&log, 3, pub_len, |_| Ok(true));
        });
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some(b));
}

/// A failed rename keeps the sealed pages. The retry rewrites every page the
/// failed attempt touched, so none of its directory slots is left behind.
#[test]
fn failed_rename_retries_the_sealed_pages() {
    let d = dir();
    let pub_len = 3 * 4096u64;
    let a = [6u8; 8];
    let b = [7u8; 8];
    let c = [8u8; 8];
    let commit_path = commit_path(&d);
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        let saved = std::fs::read(&commit_path).unwrap();
        std::fs::remove_file(&commit_path).unwrap();
        std::fs::create_dir(&commit_path).unwrap();
        log.note_slots(&[(24, b), (4096 + 16, b)]).unwrap();
        assert!(try_commit(&log, 2, |_| Ok(true)).is_err());
        assert_eq!(
            log.slot(24).unwrap(),
            Some(b),
            "a failed rename keeps the sealed slots"
        );
        std::fs::remove_dir(&commit_path).unwrap();
        std::fs::write(&commit_path, saved).unwrap();
        log.note_slots(&[(2 * 4096 + 16, c)]).unwrap();
        commit(&log, 3, pub_len, |_| Ok(true));
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(24).unwrap(), Some(b));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some(b));
    assert_eq!(log.slot(2 * 4096 + 16).unwrap(), Some(c));
}

/// The commit names the lengths. A table header that lags it, after a
/// failed header write, does not refuse open.
#[test]
fn a_lagging_table_header_does_not_refuse_open() {
    let d = dir();
    let pub_len = 4096u64;
    let a = [9u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
    }
    for name in [segment_name(1), idx_name(0), log_name(0)] {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(delta_dir(&d).join(name))
            .unwrap();
        f.seek(SeekFrom::Start(8)).unwrap();
        f.write_all(&16u64.to_le_bytes()).unwrap();
    }
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
}

/// A committed record whose slots are out of order is damage: the
/// checkpoint that merges new notes over it refuses instead of writing a
/// record readers would search wrongly.
#[test]
fn a_record_out_of_slot_order_refuses_the_merge() {
    let d = dir();
    let (a, b) = ([4u8; 8], [5u8; 8]);
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(4096 + 16, a), (4096 + 40, b)]).unwrap();
        commit(&log, 1, 2 * 4096, |_| Ok(true));
    }
    let seg = delta_dir(&d).join(segment_name(1));
    let mut bytes = std::fs::read(&seg).unwrap();
    let mut head = 4096u64.to_le_bytes().to_vec();
    head.extend_from_slice(&2u16.to_le_bytes());
    let at = bytes
        .windows(head.len())
        .position(|w| w == head.as_slice())
        .expect("page 1's record");
    let (first, second) = (at + 10, at + 20);
    let rel = |b: &[u8], i: usize| [b[i], b[i + 1]];
    let (r1, r2) = (rel(&bytes, first), rel(&bytes, second));
    bytes[first..first + 2].copy_from_slice(&r2);
    bytes[second..second + 2].copy_from_slice(&r1);
    std::fs::write(&seg, &bytes).unwrap();
    let log = reload(&d, 2 * 4096).unwrap();
    log.note_slots(&[(4096 + 56, [6u8; 8])]).unwrap();
    let err = try_commit(&log, 2, |_| Ok(true)).unwrap_err();
    assert!(
        matches!(
            err,
            rbitcoin_store::error::StoreError::Corrupt("spent.delta rel order")
        ),
        "{err}"
    );
}

/// A clip drops the directory past the body. Regrowing the directory over
/// those pages later does not bring the dropped entries back.
#[test]
fn clip_then_directory_regrow_does_not_resurrect_dropped_slots() {
    let d = dir();
    let a = [3u8; 8];
    let b = [10u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(2048, a), (4096 + 16, a)]).unwrap();
        commit(&log, 1, 2 * 4096, |_| Ok(true));
    }
    drop(reload(&d, 1024).unwrap());
    {
        let log = reload(&d, 8192).unwrap();
        log.note_slots(&[(4096 + 24, b)]).unwrap();
        commit(&log, 2, 8192, |_| Ok(true));
    }
    let log = reload(&d, 8192).unwrap();
    assert_eq!(log.slot(2048).unwrap(), Some([0u8; 8]));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some([0u8; 8]));
    assert_eq!(log.slot(4096 + 24).unwrap(), Some(b));
}

#[test]
fn crash_before_commit_rename_keeps_the_old_commit_readable() {
    let d = dir();
    let pub_len = 4096u64;
    let a = [1u8, 2, 3, 4, 5, 6, 7, 8];
    let b = [8u8, 7, 6, 5, 4, 3, 2, 1];
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, a)]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    let old = std::fs::read(commit_path(&d)).unwrap();
    log.note_slots(&[(24, b)]).unwrap();
    commit(&log, 2, pub_len, |_| Ok(true));
    std::fs::write(commit_path(&d), old).unwrap();
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(24).unwrap(), Some([0u8; 8]));
}

/// RAM changes only when the commit is installed, so a reader in the middle
/// of a checkpoint still sees the sealed slot.
#[test]
fn sealed_slots_stay_readable_until_the_commit_is_renamed() {
    let d = dir();
    let meta = [4u8, 4, 4, 4, 4, 4, 4, 4];
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, meta)]).unwrap();
    try_commit(&log, 1, |_| {
        assert_eq!(log.slot(16).unwrap(), Some(meta));
        assert_eq!(log.slot(16).unwrap(), Some(meta));
        Ok(true)
    })
    .unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(meta));
}

#[test]
fn keep_error_leaves_the_sealed_slots_in_ram() {
    let d = dir();
    let meta = [5u8, 5, 5, 5, 5, 5, 5, 5];
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, meta)]).unwrap();
    let err = try_commit(&log, 1, |_| {
        Err(rbitcoin_store::error::StoreError::Corrupt("keep probe"))
    })
    .unwrap_err();
    assert!(err.to_string().contains("keep probe"));
    assert_eq!(log.slot(16).unwrap(), Some(meta));
}

#[test]
fn clipped_straddling_page_drops_slots_past_the_body() {
    let d = dir();
    let a = [2u8, 2, 2, 2, 2, 2, 2, 2];
    let b = [3u8, 3, 3, 3, 3, 3, 3, 3];
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, a), (2048, b)]).unwrap();
    commit(&log, 1, 4096, |_| Ok(true));
    drop(log);
    let log = reload(&d, 1024).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(2048).unwrap(), Some([0u8; 8]));
    assert_eq!(log.slot(1500).unwrap(), Some([0u8; 8]));
    drop(log);
    let log = reload(&d, 4096).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(2048).unwrap(), Some([0u8; 8]));
}

#[test]
fn a_short_delta_record_is_an_error_not_an_unspent_slot() {
    let d = dir();
    let pub_len = 4096u64;
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, [6u8, 6, 6, 6, 6, 6, 6, 6])]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    let delta = delta_dir(&d).join(segment_name(1));
    let f = std::fs::OpenOptions::new()
        .write(true)
        .open(&delta)
        .unwrap();
    f.set_len(16).unwrap();
    assert!(log.slot(16).is_err());
    assert!(log.slots_at(&[16]).is_err());
}

/// A schema-29 datadir with one confirmed create, its single spend slot at
/// byte 16 of `spent.body`, and `meta` back at 29.
fn schema_29_store(d: &std::path::Path) {
    schema_29_store_with(d, |_| {});
}

/// [`schema_29_store`] with `chain` run on the new store first.
fn schema_29_store_with(d: &std::path::Path, chain: impl FnOnce(&rbitcoin_store::Store)) {
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::header_table::HeaderRecord;
    use rbitcoin_store::tx_table::{InputRecord, OutputRecord, TxRecord};

    let s = rbitcoin_store::Store::create_tiny(d).unwrap();
    chain(&s);
    let hdr = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1,
        bits: 0x1d00ffff,
        nonce: 1,
        merkle_root: [1u8; 32],
        hash: [2u8; 32],
        size: 0,
        weight: 0,
    };
    s.put_header(&hdr).unwrap();
    let item = (
        TxRecord {
            txid: [3u8; 32],
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
        vec![OutputRecord::unspent(1, vec![0x51])],
    );
    s.put_tx_full_batch_indexed(&[item], false).unwrap();
    // No block range names the create; the sync mark keeps it at open.
    s.txs.sync_class_a_named(1).unwrap();
    s.flush().unwrap();
    drop(s);
    std::fs::remove_dir_all(delta_dir(d)).unwrap();
    let mut meta = std::fs::read(d.join("meta")).unwrap();
    meta[4..6].copy_from_slice(&29u16.to_le_bytes());
    std::fs::write(d.join("meta"), meta).unwrap();
}

/// Schema-29 page log bytes: images back to back in `spend.pages` and a
/// commit naming them at generation 1 (`held:u16`, `height`, `generation`,
/// `committed_len`, `n`, then `page_off | log_off | image_len | generation`
/// records and the sealed-tip trailer).
fn write_schema_29_log(d: &std::path::Path, pages: &[(u64, Vec<u8>)], height: u32, held: bool) {
    let mut images = Vec::new();
    let mut recs = Vec::new();
    for (page_off, image) in pages {
        recs.push((*page_off, images.len() as u64, image.len() as u32));
        images.extend_from_slice(image);
    }
    std::fs::write(d.join("spend.pages"), &images).unwrap();
    let mut c = Vec::new();
    c.extend_from_slice(b"RBT1");
    c.extend_from_slice(&29u16.to_le_bytes());
    c.extend_from_slice(&u16::from(held).to_le_bytes());
    c.extend_from_slice(&height.to_le_bytes());
    c.extend_from_slice(&1u32.to_le_bytes());
    c.extend_from_slice(&(images.len() as u64).to_le_bytes());
    c.extend_from_slice(&(recs.len() as u32).to_le_bytes());
    for (page_off, log_off, len) in recs {
        c.extend_from_slice(&page_off.to_le_bytes());
        c.extend_from_slice(&log_off.to_le_bytes());
        c.extend_from_slice(&len.to_le_bytes());
        c.extend_from_slice(&0u32.to_le_bytes());
    }
    c.extend_from_slice(&height.to_le_bytes());
    std::fs::write(d.join("spend.pages.commit"), c).unwrap();
}

fn page_zero_image(meta: [u8; 8]) -> Vec<u8> {
    let mut image = vec![0u8; 16];
    image.extend_from_slice(&meta);
    image
}

/// Open converts a schema-29 page log into one delta generation. The image
/// keeps serving its slot, `spent.body` is not written, and the 29 files go.
#[test]
fn a_populated_schema_29_log_becomes_a_delta_generation() {
    let d = dir();
    schema_29_store(&d);
    let meta =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 7).unwrap();
    write_schema_29_log(&d, &[(0, page_zero_image(meta))], 0, false);
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(meta));
    let published = s.txs.spent.body_published_len();
    let mut body = [0u8; 8];
    s.txs
        .spent
        .read_prefix_at_published(published, 16, 8, &mut body)
        .unwrap();
    assert_eq!(body, [0u8; 8], "the image is not folded onto spent.body");
    drop(s);
    assert!(!d.join("spend.pages.commit").exists());
    assert!(!d.join("spend.pages").exists());
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        rbitcoin_primitives::SCHEMA_VERSION
    );
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(meta));
}

/// An upgrade whose open fails after the spend delta published its
/// adoption already says the current schema, and the 29 log is gone: no schema-29
/// binary can open the datadir and write spends the published delta would
/// never read.
#[test]
fn an_upgrade_open_that_fails_after_the_adoption_is_already_current() {
    let (_clean, d) = CleanDir::new();
    schema_29_store(&d);
    let meta =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 7).unwrap();
    write_schema_29_log(&d, &[(0, page_zero_image(meta))], 0, false);
    // The header table opens after the tx table: its refusal fails the open
    // once the adoption published.
    let path = d.join("header.body");
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[..4].copy_from_slice(b"XXXX");
    std::fs::write(&path, bytes).unwrap();
    assert!(rbitcoin_store::Store::open_tiny(&d).is_err());
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        rbitcoin_primitives::SCHEMA_VERSION
    );
    assert!(!d.join("spend.pages.commit").exists());
    assert!(!d.join("spend.pages").exists());
}

/// An upgrade that died after the first schema-30 open created
/// `spent.delta/` but before the adoption published: `meta` is still 29, so
/// the next open adopts the dense body's spends again.
#[test]
fn an_upgrade_killed_before_the_adoption_adopts_again() {
    let (_clean, d) = CleanDir::new();
    schema_29_store(&d);
    let slot =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk(9), 0).unwrap();
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.join("spent.body"))
            .unwrap();
        f.seek(SeekFrom::Start(16)).unwrap();
        f.write_all(&slot).unwrap();
    }
    // An open that fails before the adoption leaves `meta` at 29: a
    // `spent.delta` that is a file stops it there.
    std::fs::write(delta_dir(&d), b"x").unwrap();
    assert!(rbitcoin_store::Store::open_tiny(&d).is_err());
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        29,
        "meta moved to 30 before the adoption published"
    );
    std::fs::remove_file(delta_dir(&d)).unwrap();
    drop(SpendPageLog::create(&d).unwrap());
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(slot));
    drop(s);
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        rbitcoin_primitives::SCHEMA_VERSION
    );
}

/// Heights `0..20` sealed at the tip, then `confirmed[16]` nulled: the
/// crash an older binary left in the block that holds the sealed tip.
#[cfg(debug_assertions)]
fn sealed_chain_with_tip_block_hole(s: &rbitcoin_store::Store) {
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_store::header_table::{block_header_hash, HeaderRecord};

    let write = rbitcoin_store::ClassCWrite::for_test();
    let mut parent = [0u8; 32];
    let mut prev = Fk::NULL;
    for h in 0..20u32 {
        let mut merkle = [0u8; 32];
        merkle[0] = h as u8;
        let (timestamp, bits) = (h + 1, 0x207fffff);
        let rec = HeaderRecord {
            prev_fk: prev,
            version: 1,
            timestamp,
            bits,
            nonce: h,
            merkle_root: merkle,
            hash: block_header_hash(1, &parent, &merkle, timestamp, bits, h),
            size: 0,
            weight: 0,
        };
        parent = rec.hash;
        prev = s.put_header(&rec).unwrap();
        s.confirmed.set(&write, Height(h), prev).unwrap();
    }
    s.headers.flush().unwrap();
    s.flush_class_c_tip().unwrap();
    s.confirmed.set_raw_for_test(&write, Height(16), 0).unwrap();
    s.confirmed.flush().unwrap();
    rbitcoin_store::spend_durable::SpendDurable::new(17, 17)
        .store(s.path())
        .unwrap();
}

/// An upgrade that cuts a hole in the sealed tip block and stops before the
/// spend delta adopts `spent.body` leaves `meta` at 29 and arms no mark. A
/// mark would publish `meta` 31 first, and a 31 datadir with a populated
/// body and no `spent.delta/` refuses to open. The next open adopts the
/// spends, then makes the cut durable.
#[cfg(debug_assertions)]
#[test]
fn an_upgrade_stopped_after_the_tip_hole_cut_adopts_again() {
    use rbitcoin_primitives::Height;

    let (_clean, d) = CleanDir::new();
    schema_29_store_with(&d, sealed_chain_with_tip_block_hole);
    let slot =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk(9), 0).unwrap();
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.join("spent.body"))
            .unwrap();
        f.seek(SeekFrom::Start(16)).unwrap();
        f.write_all(&slot).unwrap();
    }
    // The stop: a `spent.delta` that is a file fails the open after the cut
    // and before the adoption.
    std::fs::write(delta_dir(&d), b"x").unwrap();
    rbitcoin_store::page_guard::arm_store_mark_trace();
    assert!(rbitcoin_store::Store::open_tiny(&d).is_err());
    let marks = rbitcoin_store::page_guard::take_armed_store_marks();
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        29,
        "the tip hole cut published meta before the adoption"
    );
    assert!(marks.is_empty(), "armed before the adoption: {marks:?}");
    std::fs::remove_file(delta_dir(&d)).unwrap();
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(slot));
    assert_eq!(s.confirmed.tip_height(), Some(Height(15)));
    drop(s);
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        31,
        "the cut's flush after the adoption is the first mark"
    );
}

/// A schema-30 datadir that lost `spent.delta/` refuses to open, even when
/// its sparse `spent.body` reaches the published end on disk: that length
/// cannot tell it from a body an older schema's Class A filled, and adopting
/// it would leave every spend the delta held unspent (#2038).
#[test]
fn a_schema_30_body_without_its_delta_refuses_whatever_its_length() {
    let (_clean, d) = CleanDir::new();
    schema_29_store(&d);
    {
        // The upgrade: adopts the body, writes the current `meta`.
        let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
        assert_eq!(
            rbitcoin_store::store::check_meta(&d).unwrap(),
            rbitcoin_primitives::SCHEMA_VERSION
        );
        let end = s.txs.spent.body_published_len();
        drop(s);
        std::fs::remove_dir_all(delta_dir(&d)).unwrap();
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(d.join("spent.body"))
            .unwrap();
        if f.metadata().unwrap().len() < end {
            f.set_len(end).unwrap();
        }
    }
    let err = rbitcoin_store::Store::open_tiny(&d)
        .err()
        .expect("a schema-30 body without spent.delta must refuse");
    assert!(err.to_string().contains("spent.delta is missing"), "{err}");
}

/// A held schema-29 log is not refused: refusing after `meta` is 30 would
/// leave no binary that opens the datadir. Its spends are sealed above any
/// tip, so the open filter runs `keep` over them as 29 did before a fold.
#[test]
fn a_held_schema_29_log_opens_and_is_filtered() {
    let d = dir();
    schema_29_store(&d);
    let meta =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk(9), 0).unwrap();
    write_schema_29_log(&d, &[(0, page_zero_image(meta))], 0, true);
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(
        rbitcoin_store::store::check_meta(&d).unwrap(),
        rbitcoin_primitives::SCHEMA_VERSION
    );
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(meta));
    s.drop_spends_above_tip().unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some([0u8; 8]));
}

/// The open filter checks only pages a generation wrote while sealing spends
/// above the tip, and lowers the ring so a later open does not check them
/// again.
#[test]
fn open_filter_checks_pages_sealed_above_the_tip_once() {
    let d = dir();
    let pub_len = 2 * 4096u64;
    let a = [11u8; 8];
    let b = [12u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, a)]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        log.note_slots(&[(4096 + 16, b)]).unwrap();
        commit(&log, 5, pub_len, |_| Ok(true));
    }
    let mut seen = Vec::new();
    {
        let log = reload(&d, pub_len).unwrap();
        log.drop_spends_above(3, |m| {
            seen.push(*m);
            Ok(false)
        })
        .unwrap();
        assert_eq!(log.slot(4096 + 16).unwrap(), Some([0u8; 8]));
    }
    assert_eq!(
        seen,
        vec![b],
        "only the page sealed above the tip is checked"
    );
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(4096 + 16).unwrap(), Some([0u8; 8]));
    log.drop_spends_above(3, |_| panic!("a filtered ring is not checked again"))
        .unwrap();
}

/// A schema-29 page that stayed in an older generation file is read from
/// `spend.pages.{generation}`, beside the header generation in `spend.pages`.
#[test]
fn a_schema_29_page_in_an_older_generation_file_is_adopted() {
    let d = dir();
    schema_29_store(&d);
    let a =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 3).unwrap();
    let b =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 4).unwrap();
    std::fs::write(d.join("spend.pages.2"), page_zero_image(a)).unwrap();
    let mut header_gen = vec![0u8; 16];
    header_gen.extend_from_slice(&b);
    std::fs::write(d.join("spend.pages"), &header_gen).unwrap();
    let mut c = Vec::new();
    c.extend_from_slice(b"RBT1");
    c.extend_from_slice(&29u16.to_le_bytes());
    c.extend_from_slice(&0u16.to_le_bytes());
    c.extend_from_slice(&0u32.to_le_bytes());
    c.extend_from_slice(&3u32.to_le_bytes());
    c.extend_from_slice(&(header_gen.len() as u64).to_le_bytes());
    c.extend_from_slice(&2u32.to_le_bytes());
    for (page_off, log_off, len, generation) in [(0u64, 0u64, 24u32, 2u32), (4096, 8, 16, 0)] {
        c.extend_from_slice(&page_off.to_le_bytes());
        c.extend_from_slice(&log_off.to_le_bytes());
        c.extend_from_slice(&len.to_le_bytes());
        c.extend_from_slice(&generation.to_le_bytes());
    }
    std::fs::write(d.join("spend.pages.commit"), c).unwrap();
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(a));
    assert!(!d.join("spend.pages.2").exists());
}

/// A checkpoint runs `keep` over the slots it seals, not over every older
/// slot on the page: readers already require a best-chain spender, and the
/// open filter covers a spender a crash cut.
#[test]
fn checkpoint_keeps_only_the_slots_it_seals() {
    let d = dir();
    let pub_len = 4096u64;
    let a = [13u8; 8];
    let b = [14u8; 8];
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, a)]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    let mut seen = Vec::new();
    commit(&log, 2, pub_len, |m| {
        seen.push(*m);
        Ok(*m != a)
    });
    log.note_slots(&[(24, b)]).unwrap();
    commit(&log, 3, pub_len, |m| {
        seen.push(*m);
        Ok(*m != a)
    });
    assert_eq!(seen, vec![b]);
    assert_eq!(log.slot(16).unwrap(), Some(a));
    assert_eq!(log.slot(24).unwrap(), Some(b));
}

/// Two pages sealed in one checkpoint already have records, written a
/// generation apart so they do not touch. The merge reads both in one
/// completion-session batch. A pread per record leaves the bulk count at
/// zero, and one note per span leaves it at two.
#[test]
fn checkpoint_merge_reads_records_in_one_batch() {
    use rbitcoin_store::spend_delta::{delta_record_io, reset_delta_record_io};

    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let pages = [16u64, 4096 + 16];
    for (i, abs) in pages.iter().enumerate() {
        log.note_slots(&[(*abs, [i as u8 + 1; 8])]).unwrap();
        commit(&log, (i + 1) as u32, 0, |_| Ok(true));
    }
    reset_delta_record_io();
    log.note_slots(&[(pages[0], [3; 8]), (pages[1], [4; 8])])
        .unwrap();
    commit(&log, 3, 0, |_| Ok(true));
    assert_eq!(
        delta_record_io(),
        (1, 0),
        "two earlier records are one bulk submission, not a pread each"
    );
    assert_eq!(log.slot(pages[0]).unwrap(), Some([3; 8]));
    assert_eq!(log.slot(pages[1]).unwrap(), Some([4; 8]));
}

/// An open-note cap small enough that reaching it costs nothing. The cap's
/// own sizing is `a_window_after_the_cap_fills_past_it_without_growing`'s,
/// at the production [`OPEN_EDGE_CAP`].
///
/// [`OPEN_EDGE_CAP`]: rbitcoin_store::spend_delta::OPEN_EDGE_CAP
const EDGE_CAP: u64 = 1_024;

/// A freeze empties the open notes, so it releases a confirm thread parked
/// at the edge cap before the checkpoint writes anything.
#[test]
fn freeze_releases_an_edge_cap_waiter() {
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    let d = dir();
    let policy = Policy {
        open_edge_cap: EDGE_CAP,
        ..Policy::DEFAULT
    };
    let log = Arc::new(SpendPageLog::create_with(&d, policy).unwrap());
    let cap = EDGE_CAP;
    let slots: Vec<(u64, [u8; 8])> = (0..cap).map(|i| (16 + i * 8, [1u8; 8])).collect();
    log.note_slots(&slots).unwrap();
    log.bind_sync_wake(Arc::new((Mutex::new(false), Condvar::new())));
    let (tx, rx) = mpsc::channel();
    let waiter = Arc::clone(&log);
    std::thread::spawn(move || {
        waiter.wait_open_edge_room().unwrap();
        tx.send(()).unwrap();
    });
    assert!(rx
        .recv_timeout(std::time::Duration::from_millis(200))
        .is_err());
    log.checkpoint(
        |l| {
            l.freeze_open(0, None::<fn(&[u8; 8]) -> _>)?;
            rx.recv_timeout(std::time::Duration::from_secs(10))
                .expect("the freeze releases the edge-cap waiter");
            Ok(())
        },
        &mut MemBody::new(0),
        None,
    )
    .unwrap();
}

/// After a window that reached the cap, the next window's map takes the cap
/// and a write phase's worth of notes past it (a full `writeq` of runs,
/// about 450,000) without growing: `notes_ram=` holds still.
///
/// This journey takes about 2.5 s in a debug build: it notes 4.45 million
/// slots. The contract is that the fixed 2,000,000 [`OPEN_EDGE_CAP`] plus a
/// write phase fits the map one cap window sizes. A smaller window sizes a
/// smaller map and never reaches the room limit, so no smaller N tests it.
///
/// [`OPEN_EDGE_CAP`]: rbitcoin_store::spend_delta::OPEN_EDGE_CAP
#[test]
fn a_window_after_the_cap_fills_past_it_without_growing() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let cap = rbitcoin_store::spend_delta::OPEN_EDGE_CAP;
    let note = |from: u64, n: u64| {
        let slots: Vec<(u64, [u8; 8])> = (from..from + n)
            .map(|i| (16 + i * 56, (i + 1).to_le_bytes()))
            .collect();
        for batch in slots.chunks(8_000) {
            log.note_slots(batch).unwrap();
        }
    };
    note(0, cap);
    commit(&log, 1, 0, |_| Ok(true));
    let sized = log.stats().notes_bytes;
    assert!(sized <= 72 << 20, "{sized}");
    note(cap, cap + 450_000);
    assert_eq!(log.stats().notes_bytes, sized);
    assert_eq!(
        log.slot(16 + 56 * cap).unwrap(),
        Some((cap + 1).to_le_bytes())
    );
}

/// A node at the tip notes a block or so a window: its notes stay small,
/// not the size of a window at the cap.
#[test]
fn a_tip_window_keeps_a_small_map() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    for (tip, from) in [(1u32, 0u64), (2, 5_000)] {
        let slots: Vec<(u64, [u8; 8])> = (from..from + 5_000)
            .map(|i| (16 + i * 4096, (i + 1).to_le_bytes()))
            .collect();
        log.note_slots(&slots).unwrap();
        commit(&log, tip, 0, |_| Ok(true));
        let ram = log.stats().notes_bytes;
        assert!(ram <= 256 << 10, "{ram}");
    }
}

/// The page directory is spend-delta RAM too: its bytes follow the
/// highest page a generation names, eight bytes a page.
#[test]
fn stats_meter_the_page_directory() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(1000 * 4096 + 16, [1u8; 8])]).unwrap();
    commit(&log, 1, 0, |_| Ok(true));
    let bytes = log.stats().directory_bytes;
    assert!((1001 * 8..4 * 1001 * 8).contains(&bytes), "{bytes}");
}

/// Across two checkpoints that fail after their freeze, the newest note for
/// a slot wins in every layer: a frozen note over the sealed one while the
/// checkpoint runs and after it merges them, an open note over both. After
/// a disconnect, a rejected note held only in the sealed notes becomes a
/// zero, and that survives a reload.
#[test]
fn the_newest_note_wins_across_failed_checkpoints() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let (a, b) = (16u64, 4096 * 5 + 24);
    let (x, y, z) = ([1u8; 8], [2u8; 8], [3u8; 8]);
    let injected = || Err(rbitcoin_store::error::StoreError::Corrupt("injected"));
    log.note_slots(&[(a, x), (b, x)]).unwrap();
    let first = log.checkpoint(
        |l| {
            l.freeze_open(1, None::<fn(&[u8; 8]) -> _>)?;
            injected()
        },
        &mut MemBody::new(0),
        None,
    );
    assert!(first.is_err());
    log.note_slots(&[(a, y)]).unwrap();
    let second = log.checkpoint(
        |l| {
            l.freeze_open(2, None::<fn(&[u8; 8]) -> _>)?;
            assert_eq!(l.slot(a).unwrap(), Some(y), "frozen over sealed");
            injected()
        },
        &mut MemBody::new(0),
        None,
    );
    assert!(second.is_err());
    assert_eq!(log.slot(a).unwrap(), Some(y), "merged: frozen over sealed");
    assert_eq!(log.slot(b).unwrap(), Some(x));
    log.note_slots(&[(a, z)]).unwrap();
    assert_eq!(log.slot(a).unwrap(), Some(z), "open over sealed");
    commit(&log, 3, 0, |m| Ok(*m != x));
    assert_eq!(log.slot(a).unwrap(), Some(z));
    assert_eq!(log.slot(b).unwrap(), Some([0u8; 8]), "rejected sealed note");
    drop(log);
    let log = reload(&d, 1 << 20).unwrap();
    assert_eq!(log.slot(a).unwrap(), Some(z));
    assert_eq!(log.slot(b).unwrap(), Some([0u8; 8]));
}

/// A checkpoint that fails after its freeze holds the frozen notes sorted,
/// sixteen bytes a note plus a directory of well under one byte a note,
/// reads them, and a retry publishes them.
#[test]
fn a_failed_checkpoint_holds_its_notes_sorted() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let n = 100_000u64;
    let slots: Vec<(u64, [u8; 8])> = (0..n)
        .map(|i| (16 + i * 4096 * 3 + (i % 7) * 8, (i + 1).to_le_bytes()))
        .collect();
    log.note_slots(&slots).unwrap();
    let failed = log.checkpoint(
        |l| {
            l.freeze_open(1, None::<fn(&[u8; 8]) -> _>)?;
            Err(rbitcoin_store::error::StoreError::Corrupt("injected"))
        },
        &mut MemBody::new(0),
        None,
    );
    assert!(failed.is_err());
    let failed_ram = log.stats().notes_bytes;
    let abs: Vec<u64> = slots.iter().map(|&(a, _)| a).collect();
    let want: Vec<Option<[u8; 8]>> = slots.iter().map(|&(_, m)| Some(m)).collect();
    assert_eq!(log.slots_at(&abs).unwrap(), want);
    commit(&log, 2, 0, |_| Ok(true));
    let held = failed_ram - log.stats().notes_bytes;
    assert!(held >= 16 * n && held <= 16 * n + n / 2, "{held}");
    assert_eq!(log.slots_at(&abs).unwrap(), want);
}

/// A commit from an earlier schema-30 build refuses open with a wipe line
/// instead of being read with this layout.
#[test]
fn an_earlier_build_commit_refuses_open() {
    let d = dir();
    drop(SpendPageLog::create(&d).unwrap());
    let mut old = std::fs::read(commit_path(&d)).unwrap();
    old.truncate(32);
    std::fs::write(commit_path(&d), old).unwrap();
    let err = reload(&d, 1 << 20)
        .err()
        .expect("an earlier layout refuses");
    assert!(err.to_string().contains("wipe datadir"), "{err}");
}

/// A layout-3 commit, from builds whose `idx` entries carried no filled-slot
/// count, refuses open with the wipe line.
#[test]
fn a_layout_3_commit_refuses_open() {
    let d = dir();
    drop(SpendPageLog::create(&d).unwrap());
    let mut old = std::fs::read(commit_path(&d)).unwrap();
    old[6..8].copy_from_slice(&3u16.to_le_bytes());
    old.truncate(128);
    std::fs::write(commit_path(&d), old).unwrap();
    let err = reload(&d, 1 << 20).err().expect("layout 3 refuses");
    assert!(err.to_string().contains("wipe datadir"), "{err}");
}

/// An open that stopped after the adopt and before the 29 files were removed
/// does not adopt them again over what the delta already holds.
#[test]
fn schema_29_files_left_after_an_adopt_are_only_removed() {
    let d = dir();
    schema_29_store(&d);
    let first =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 1).unwrap();
    let second =
        rbitcoin_store::tx_table::encode_spent_slot(0, rbitcoin_primitives::Fk::NULL, 2).unwrap();
    write_schema_29_log(&d, &[(0, page_zero_image(first))], 0, false);
    drop(rbitcoin_store::Store::open_tiny(&d).unwrap());
    write_schema_29_log(&d, &[(0, page_zero_image(second))], 0, false);
    let s = rbitcoin_store::Store::open_tiny(&d).unwrap();
    assert_eq!(s.spend_directory_slot(16).unwrap(), Some(first));
    assert!(!d.join("spend.pages.commit").exists());
    assert!(!d.join("spend.pages").exists());
}

/// When every ring generation is above the tip, an older generation may be
/// too, so the open filter checks a page whose generation left the ring.
#[test]
fn open_filter_checks_every_page_when_the_ring_is_all_above_the_tip() {
    let d = dir();
    let pub_len = 2 * 4096u64;
    let old = [21u8; 8];
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&[(16, old)]).unwrap();
        commit(&log, 9, pub_len, |_| Ok(true));
        for i in 0..8u8 {
            log.note_slots(&[(4096 + 16, [30 + i; 8])]).unwrap();
            commit(&log, 9, pub_len, |_| Ok(true));
        }
    }
    let log = reload(&d, pub_len).unwrap();
    let mut seen = Vec::new();
    log.drop_spends_above(3, |m| {
        seen.push(*m);
        Ok(true)
    })
    .unwrap();
    assert!(seen.contains(&old), "the page outside the ring is checked");
}

/// Tiny segments and a slope so steep that any delta is past the ceiling, so
/// a few pages roll and every record folds.
/// Two one-slot records per segment.
const SMALL: Policy = Policy {
    segment_cap: 48,
    reclaim_from_segments: 3,
    keep_segments: 2,
    sparse_permille: 50,
    reclaim_slope: u64::MAX,
    reclaim_rate: 1,
    fold_round_pages: usize::MAX,
    log_fold_min: 4 << 20,
    open_edge_cap: rbitcoin_store::spend_delta::OPEN_EDGE_CAP,
};

/// Below the ceiling at any size: reclaim every segment not wholly live, and
/// fold only the records the fold rule picks.
const CARRY: Policy = Policy {
    reclaim_slope: 0,
    sparse_permille: 999,
    reclaim_from_segments: 2,
    keep_segments: 1,
    ..SMALL
};

/// A slot value that marks a body slot spent before the delta saw it.
const SPENT: [u8; 8] = [0xee, 0, 0, 0, 0, 0, 0, 1];

/// `spent.body` with every slot already spent except slot 2 of each page,
/// the one [`page_meta`] notes: a one-slot record spends all its page has
/// left, so the fold rule folds it once the log has counted the append.
fn spent_body(pages: u64) -> MemBody {
    let mut body = MemBody::new((pages * 4096) as usize);
    for slot in body.bytes.chunks_exact_mut(8) {
        slot.copy_from_slice(&SPENT);
    }
    for page in 0..pages {
        let at = (page * 4096 + 16) as usize;
        body.bytes[at..at + 8].fill(0);
    }
    body
}

const PAGES: u64 = 64;

fn page_meta(page: u64, round: u8) -> [u8; 8] {
    [page as u8 + 1, round, 0, 0, 0, 0, 0, 1]
}

/// What a reader sees: the overlay, else `spent.body`.
fn read_through(log: &SpendPageLog, body: &MemBody, abs: u64) -> [u8; 8] {
    log.slot(abs).unwrap().unwrap_or_else(|| body.slot(abs))
}

/// One checkpoint per page, each with one slot, every one below the marker.
fn churn(d: &std::path::Path, body: &mut MemBody, durable: Option<u32>) -> SpendPageLog {
    let log = SpendPageLog::create_with(d, SMALL).unwrap();
    for page in 0..PAGES {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        folding_commit(&log, page as u32 + 1, |_| Ok(true), body, durable).unwrap();
    }
    log
}

/// The delta files sit in one directory under the store.
#[test]
fn delta_files_live_in_their_own_directory() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, [1u8; 8])]).unwrap();
    commit(&log, 1, 4096, |_| Ok(true));
    let top: Vec<String> = std::fs::read_dir(&d)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(top, vec![SPEND_DELTA_DIR.to_string()]);
    let mut inner: Vec<String> = std::fs::read_dir(delta_dir(&d))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    inner.sort();
    assert_eq!(inner, vec!["commit", "idx.0", "log.0", "seg.1"]);
}

/// A segment rolls at the cap, and one no entry names any more is unlinked.
#[test]
fn rewritten_pages_leave_dead_segments_that_are_unlinked() {
    let d = dir();
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    for round in 1..=12u8 {
        log.note_slots(&[(16, [round; 8])]).unwrap();
        try_commit(&log, u32::from(round), |_| Ok(true)).unwrap();
    }
    assert_eq!(log.slot(16).unwrap(), Some([12u8; 8]));
    let stats = log.stats();
    assert!(stats.segments <= 2, "dead segments stay: {stats:?}");
    assert!(!delta_dir(&d).join(segment_name(1)).exists());
    drop(log);
    let mut body = MemBody::new(4096);
    let log = SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    assert_eq!(log.slot(16).unwrap(), Some([12u8; 8]));
}

/// Past the budget, segments fold into `spent.body`. Every slot
/// still reads its value, the delta stays near its budget, and at least two
/// segments stay.
#[test]
fn old_segments_fold_into_spent_body_under_budget() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let log = churn(&d, &mut body, Some(u32::MAX));
    let stats = log.stats();
    assert!(stats.fold_records > 0, "{stats:?}");
    assert!(stats.segments >= 2, "{stats:?}");
    // Each segment's first block holds only its header.
    assert!(
        stats.segment_bytes <= stats.segments * 4096 + 262 + 3 * 48,
        "{stats:?}"
    );
    assert!(body.syncs > 0, "a fold syncs spent.body before its commit");
    for page in 0..PAGES {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 1)
        );
    }
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    for page in 0..PAGES {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 1)
        );
    }
    assert_eq!(log.stats().fold_records, stats.fold_records);
    assert!(log.stats().written() > 0);
}

/// A record sealed above the durable marker is not folded: in `spent.body`
/// the open filter could not drop it after a crash.
#[test]
fn fold_waits_for_the_durable_marker() {
    for durable in [None, Some(0)] {
        let d = dir();
        let mut body = MemBody::new((PAGES * 4096) as usize);
        let log = churn(&d, &mut body, durable);
        assert_eq!(log.stats().fold_records, 0, "durable {durable:?}");
        assert!(body.bytes.iter().all(|&b| b == 0));
    }
}

/// Page offsets a `fold` file names: `count` at byte 8, then
/// `(page_off, image)` pairs, each image a whole page.
fn fold_file_pages(d: &std::path::Path) -> Vec<u64> {
    let raw = std::fs::read(delta_dir(d).join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME))
        .unwrap();
    let count = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
    let mut at = 12;
    let mut pages = Vec::new();
    for _ in 0..count {
        let page_off = u64::from_le_bytes(raw[at..at + 8].try_into().unwrap());
        pages.push(page_off);
        at += 8 + 4096;
    }
    pages
}

/// A crash while a fold copies its pages can tear them. Every slot on those
/// pages survives, the ones only `spent.body` held included: the composed
/// pages are in `fold`, and open copies them again.
#[test]
fn a_torn_fold_is_copied_again_at_open() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let only_body = [0xa5u8; 8];
    for page in 0..PAGES {
        let at = (page * 4096 + 24) as usize;
        body.bytes[at..at + 8].copy_from_slice(&only_body);
    }
    body.fail_writes = true;
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    let mut failed = false;
    for page in 0..PAGES {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        if folding_commit(
            &log,
            page as u32 + 1,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .is_err()
        {
            failed = true;
            break;
        }
    }
    assert!(failed, "the churn reaches a fold");
    drop(log);
    let torn = fold_file_pages(&d);
    assert!(!torn.is_empty());
    for &page_off in &torn {
        body.bytes[page_off as usize..(page_off + 4096) as usize].fill(0);
    }
    body.fail_writes = false;
    let log = SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    for &page_off in &torn {
        let page = page_off / 4096;
        assert_eq!(read_through(&log, &body, page_off + 16), page_meta(page, 1));
        assert_eq!(
            body.slot(page_off + 24),
            only_body,
            "page {page} keeps its body-only slot"
        );
    }
    assert!(!delta_dir(&d)
        .join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME)
        .exists());
}

/// A `fold` temp file that was never renamed is not a fold: open leaves
/// `spent.body` alone.
#[test]
fn an_unrenamed_fold_file_is_not_copied() {
    let d = dir();
    drop(SpendPageLog::create(&d).unwrap());
    let fold = delta_dir(&d).join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME);
    std::fs::write(rbitcoin_store::file::tmp_sidecar_path(&fold), b"RBT1 torn").unwrap();
    let mut body = MemBody::new(4096);
    body.fail_writes = true;
    SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    assert!(body.bytes.iter().all(|&b| b == 0));
}

/// A segment file a failed roll left past the active segment does not stop
/// the next roll.
#[test]
fn a_leftover_segment_does_not_block_the_next_roll() {
    let d = dir();
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    std::fs::write(
        delta_dir(&d).join(segment_name(2)),
        b"left by a failed roll",
    )
    .unwrap();
    for round in 1..=4u8 {
        log.note_slots(&[(16 + u64::from(round) * 8, [round; 8])])
            .unwrap();
        try_commit(&log, u32::from(round), |_| Ok(true)).unwrap();
    }
    assert!(log.stats().segments >= 2 || !delta_dir(&d).join(segment_name(1)).exists());
    assert_eq!(log.slot(16 + 4 * 8).unwrap(), Some([4u8; 8]));
}

/// A fold takes only segments whose live records were all sealed at or
/// below the durable marker. The marker trails three checkpoints here, and
/// no page sealed above it is folded. With no budget, reclaim from two
/// segments, and a floor of one, only the marker keeps the recent segment.
#[test]
fn fold_takes_only_records_sealed_at_or_below_the_marker() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let policy = Policy {
        reclaim_from_segments: 2,
        keep_segments: 1,
        ..SMALL
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    for page in 0..PAGES {
        let tip = page as u32 + 1;
        let durable = tip.saturating_sub(3);
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        folding_commit(&log, tip, |_| Ok(true), &mut body, Some(durable)).unwrap();
        for sealed in 0..=page {
            if sealed as u32 + 1 > durable {
                assert!(
                    log.slot(sealed * 4096 + 16).unwrap().is_some(),
                    "page {sealed}, sealed at tip {} above durable {durable}, was folded",
                    sealed + 1
                );
            }
        }
    }
    assert!(log.stats().fold_records > 0);
}

/// A folded page reads `spent.body`. A later note on it is a record of just
/// that note over the body, and a crash before that commit keeps the
/// tombstone, not the slot it replaced.
#[test]
fn a_note_on_a_folded_page_reads_over_the_body() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let log = churn(&d, &mut body, Some(u32::MAX));
    assert_eq!(log.slot(4096 + 16).unwrap(), None, "page 1 was folded");
    assert_eq!(body.slot(4096 + 16), page_meta(1, 1));
    let saved = std::fs::read(commit_path(&d)).unwrap();
    log.note_slots(&[(4096 + 24, [9u8; 8])]).unwrap();
    folding_commit(&log, 100, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!(log.slot(4096 + 24).unwrap(), Some([9u8; 8]));
    assert_eq!(log.slot(4096 + 16).unwrap(), None);
    std::fs::write(commit_path(&d), saved).unwrap();
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    assert_eq!(log.slot(4096 + 24).unwrap(), None);
    assert_eq!(read_through(&log, &body, 4096 + 16), page_meta(1, 1));
}

/// Open deletes segment files nothing names.
#[test]
fn unnamed_segments_are_removed_at_open() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[(16, [3u8; 8])]).unwrap();
    commit(&log, 1, 4096, |_| Ok(true));
    drop(log);
    std::fs::write(delta_dir(&d).join(segment_name(99)), b"stray").unwrap();
    let log = reload(&d, 1 << 20).unwrap();
    assert!(!delta_dir(&d).join(segment_name(99)).exists());
    assert_eq!(log.slot(16).unwrap(), Some([3u8; 8]));
}

/// A first open that stopped between the empty commit and its files is
/// built again, not refused: on a schema-29 datadir `meta` is already 30.
#[test]
fn an_interrupted_first_open_is_built_again() {
    let d = dir();
    drop(SpendPageLog::create(&d).unwrap());
    for name in [segment_name(1), idx_name(0), log_name(0)] {
        std::fs::remove_file(delta_dir(&d).join(name)).unwrap();
    }
    let log = reload(&d, 1 << 20).unwrap();
    log.note_slots(&[(16, [5u8; 8])]).unwrap();
    commit(&log, 1, 4096, |_| Ok(true));
    drop(log);
    assert_eq!(
        reload(&d, 1 << 20).unwrap().slot(16).unwrap(),
        Some([5u8; 8])
    );
}

/// A page not wholly below the published end stays in the delta, even in an
/// old eligible segment: confirm may still write pre-fill on it, and a fold
/// must not move `spent.body`'s end.
#[test]
fn a_page_past_the_published_end_is_not_folded() {
    let d = dir();
    let short = PAGES - 10;
    let mut body = MemBody::new((short * 4096 + 8) as usize);
    let log = churn(&d, &mut body, Some(u32::MAX));
    assert!(log.stats().fold_records > 0);
    for page in short..PAGES - 2 {
        assert_eq!(
            log.slot(page * 4096 + 16).unwrap(),
            Some(page_meta(page, 1)),
            "page {page} past the published end stays in the delta"
        );
    }
}

/// Nothing folds while every segment is denser than the reclaim threshold,
/// or while there are fewer segments than the reclaim floor.
#[test]
fn fold_waits_for_the_threshold_and_the_segment_floor() {
    for policy in [
        Policy {
            reclaim_slope: 0,
            ..SMALL
        },
        Policy {
            reclaim_from_segments: 1000,
            ..SMALL
        },
    ] {
        let d = dir();
        let mut body = spent_body(PAGES);
        let log = SpendPageLog::create_with(&d, policy).unwrap();
        for page in 0..PAGES {
            log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
                .unwrap();
            folding_commit(
                &log,
                page as u32 + 1,
                |_| Ok(true),
                &mut body,
                Some(u32::MAX),
            )
            .unwrap();
        }
        assert_eq!(log.stats().fold_records, 0, "{policy:?}");
    }
}

/// One slot on each page, in one checkpoint with no marker so nothing folds.
/// A segment holds two records: pages `2k` and `2k + 1` share one. Each
/// later generation starts a new segment, since an aligned start leaves no
/// room in a cap this small.
fn fill_without_folding(d: &std::path::Path, policy: Policy, body: &mut MemBody) -> SpendPageLog {
    let log = SpendPageLog::create_with(d, policy).unwrap();
    log.adopt_v29(Vec::new(), 0, body).unwrap();
    let notes: Vec<(u64, [u8; 8])> = (0..PAGES)
        .map(|page| (page * 4096 + 16, page_meta(page, 1)))
        .collect();
    log.note_slots(&notes).unwrap();
    folding_commit(&log, PAGES as u32, |_| Ok(true), body, None).unwrap();
    assert_eq!(log.stats().fold_records, 0);
    log
}

/// A victim holding the old record of a page this checkpoint rewrites is
/// still reclaimed, since that record dies with the publish. The fold stops
/// at its cap after one segment instead of taking every eligible one.
#[test]
fn a_rewritten_page_does_not_hold_its_segment_in_the_fold() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let log = fill_without_folding(&d, SMALL, &mut body);
    log.note_slots(&[(16, page_meta(0, 2))]).unwrap();
    folding_commit(
        &log,
        PAGES as u32 + 1,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX),
    )
    .unwrap();
    assert_eq!(log.stats().fold_records, 1, "only page 1 folds");
    assert_eq!(log.slot(4096 + 16).unwrap(), None);
    assert_eq!(body.slot(4096 + 16), page_meta(1, 1));
    assert_eq!(log.slot(16).unwrap(), Some(page_meta(0, 2)));
    for page in 2..PAGES {
        assert_eq!(
            log.slot(page * 4096 + 16).unwrap(),
            Some(page_meta(page, 1)),
            "page {page} stays in the delta"
        );
    }
}

/// Under budget, a segment whose live records are a small share of its bytes
/// still folds, one cap at a time. A dense segment does not.
#[test]
fn a_sparse_segment_folds_under_budget() {
    let d = dir();
    let mut body = spent_body(PAGES);
    let policy = Policy {
        reclaim_slope: 0,
        sparse_permille: 600,
        ..SMALL
    };
    let log = fill_without_folding(&d, policy, &mut body);
    let rewritten: Vec<(u64, [u8; 8])> = (0..8u64)
        .map(|k| (2 * k * 4096 + 16, page_meta(2 * k, 2)))
        .collect();
    log.note_slots(&rewritten).unwrap();
    folding_commit(&log, PAGES as u32 + 1, |_| Ok(true), &mut body, None).unwrap();
    for round in 0..20u32 {
        // A page past the body: no committed record of its own to fold.
        log.note_slots(&[(PAGES * 4096 + 16, [round as u8 + 3; 8])])
            .unwrap();
        folding_commit(
            &log,
            PAGES as u32 + 2 + round,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .unwrap();
        if round == 0 {
            assert_eq!(log.stats().fold_records, 1, "one segment per cap");
        }
    }
    for k in 0..8u64 {
        let page = 2 * k + 1;
        assert_eq!(log.slot(page * 4096 + 16).unwrap(), None, "page {page}");
        assert_eq!(body.slot(page * 4096 + 16), page_meta(page, 1));
    }
    for page in 16..PAGES {
        assert_eq!(
            log.slot(page * 4096 + 16).unwrap(),
            Some(page_meta(page, 1)),
            "page {page} in a dense segment stays in the delta"
        );
    }
}

/// Over budget, the sparsest segment folds first: it frees the most delta
/// bytes for the fewest `spent.body` pages.
#[test]
fn the_sparsest_segment_folds_first() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    // Four one-slot records to a segment.
    let policy = Policy {
        segment_cap: 16 + 4 * 20,
        sparse_permille: 0,
        ..SMALL
    };
    let log = fill_without_folding(&d, policy, &mut body);
    // Segment 1 holds pages 0..4 and keeps three live; segment 2 holds
    // pages 4..8 and keeps one.
    let rewritten: Vec<(u64, [u8; 8])> = [0u64, 4, 5, 6]
        .iter()
        .map(|&page| (page * 4096 + 16, page_meta(page, 2)))
        .collect();
    log.note_slots(&rewritten).unwrap();
    folding_commit(&log, PAGES as u32 + 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(PAGES / 2 * 4096 + 16, page_meta(PAGES / 2, 2))])
        .unwrap();
    folding_commit(
        &log,
        PAGES as u32 + 2,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX),
    )
    .unwrap();
    assert_eq!(log.stats().fold_records, 1);
    assert_eq!(log.slot(7 * 4096 + 16).unwrap(), None, "page 7 folds");
    for page in 1..4u64 {
        assert!(log.slot(page * 4096 + 16).unwrap().is_some(), "page {page}");
    }
}

/// Below the ceiling, reclaim folds a record once its spends cover at least
/// half the slots its `spent.body` page still has unset, and copies any
/// other, or one on a page not wholly below the published end, to the active
/// segment. Either way the old segment goes.
///
/// - page 0: 256 spends over a fresh page's 510 slots: folds;
/// - page 1: one spend over a fresh page: carried;
/// - page 2: 32 spends over the 64 slots its folded page has left: folds,
///   though it covers a sixteenth of the page;
/// - page 3: 100 spends over 212 unset slots, plus 50 explicit zeroes, which
///   do not count: carried, though it overrides 150 slots;
/// - page `last`: one spend, straddling the end: carried.
#[test]
fn a_record_folds_once_it_spends_half_its_page_s_unset_slots() {
    let d = dir();
    let last = PAGES - 1;
    let mut body = MemBody::new((last * 4096 + 2048) as usize);
    let spent_from = |page: u64, unset: u64, body: &mut MemBody| {
        for rel in (0..2).chain(2 + unset..512) {
            let at = (page * 4096 + rel * 8) as usize;
            body.bytes[at..at + 8].copy_from_slice(&SPENT);
        }
    };
    spent_from(2, 64, &mut body);
    spent_from(3, 212, &mut body);
    let slot = |page: u64, rel: u64| page * 4096 + rel * 8;
    let meta = |rel: u64, tag: u8| [rel as u8, tag, 0, 0, 0, 0, 0, 1];
    let folds: Vec<(u64, [u8; 8])> = (2..258u64)
        .map(|rel| (slot(6, rel), meta(rel, 7)))
        .chain((2..34u64).map(|rel| (slot(2, rel), meta(rel, 7))))
        .collect();
    let carried: Vec<(u64, [u8; 8])> = (2..102u64)
        .map(|rel| (slot(3, rel), meta(rel, 7)))
        .chain((406..456u64).map(|rel| (slot(3, rel), [0u8; 8])))
        .chain([
            (slot(1, 2), page_meta(1, 1)),
            (slot(last, 2), page_meta(last, 1)),
        ])
        .collect();
    // Rewritten by the next checkpoint, so segment 1 is not wholly live.
    let dies = (slot(7, 2), page_meta(7, 1));
    let policy = Policy {
        // Every record above fills segment 1.
        segment_cap: 6 * 10 + (folds.len() + carried.len() + 1) as u64 * 10,
        ..CARRY
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    log.note_slots(&[folds.clone(), carried.clone(), vec![dies]].concat())
        .unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(slot(7, 2), page_meta(7, 2))]).unwrap();
    folding_commit(&log, 2, |_| Ok(true), &mut body, None).unwrap();
    assert!(delta_dir(&d).join(segment_name(1)).exists());
    log.note_slots(&[(slot(5, 2), page_meta(5, 1))]).unwrap();
    folding_commit(&log, 3, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(log.stats().fold_records, 2, "pages 6 and 2 fold");
    assert!(!delta_dir(&d).join(segment_name(1)).exists());
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, policy).unwrap();
    for &(abs, meta) in &folds {
        assert_eq!(log.slot(abs).unwrap(), None, "{abs}");
        assert_eq!(body.slot(abs), meta);
    }
    for &(abs, meta) in &carried {
        assert_eq!(log.slot(abs).unwrap(), Some(meta), "{abs}");
        assert_ne!(body.slot(abs), meta, "{abs}");
    }
}

/// Below the ceiling, a segment still mostly live is left alone: reclaiming
/// it would write about as many bytes as it frees.
#[test]
fn a_mostly_live_segment_is_not_reclaimed() {
    let d = dir();
    let mut body = spent_body(PAGES);
    let policy = Policy {
        reclaim_slope: 0,
        sparse_permille: 500,
        ..SMALL
    };
    let log = fill_without_folding(&d, policy, &mut body);
    let before = log.stats().segments;
    // A page past the body: no committed record of its own to fold.
    log.note_slots(&[(PAGES * 4096 + 16, page_meta(PAGES, 2))])
        .unwrap();
    folding_commit(
        &log,
        PAGES as u32 + 1,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX),
    )
    .unwrap();
    assert_eq!(log.stats().fold_records, 0);
    assert!(log.stats().segments >= before, "{:?}", log.stats());
}

/// Open may cut `spent.body` back to its durable Class A end after a fold
/// wrote `fold`. Replay copies each image only below the new end, and open
/// still succeeds.
#[test]
fn a_fold_file_replays_onto_a_shorter_body() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    for page in 0..PAGES {
        let at = (page * 4096 + 24) as usize;
        body.bytes[at..at + 8].copy_from_slice(&[0xa5u8; 8]);
    }
    body.fail_writes = true;
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    for page in 0..PAGES {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        if folding_commit(
            &log,
            page as u32 + 1,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .is_err()
        {
            break;
        }
    }
    drop(log);
    let folded = fold_file_pages(&d);
    let last = *folded.iter().max().unwrap();
    body.bytes.truncate((last + 2048) as usize);
    body.fail_writes = false;
    SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    for &page_off in &folded {
        assert_eq!(body.slot(page_off + 16), page_meta(page_off / 4096, 1));
    }
    assert!(!delta_dir(&d)
        .join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME)
        .exists());
}

/// A failed publish that carried records leaves their pages' spare slots
/// stamped. A retry that carries nothing must not leave those slots to be
/// selected after reload, naming a segment the log no longer keeps.
#[test]
fn a_failed_carry_leaves_no_slot_behind() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let policy = CARRY;
    let log = fill_without_folding(&d, policy, &mut body);
    let commit_path = commit_path(&d);
    let saved = std::fs::read(&commit_path).unwrap();
    std::fs::remove_file(&commit_path).unwrap();
    std::fs::create_dir(&commit_path).unwrap();
    let last = PAGES - 1;
    log.note_slots(&[(last * 4096 + 16, page_meta(last, 2))])
        .unwrap();
    assert!(folding_commit(
        &log,
        PAGES as u32 + 1,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX)
    )
    .is_err());
    std::fs::remove_dir(&commit_path).unwrap();
    std::fs::write(&commit_path, saved).unwrap();
    for round in 0..12u8 {
        log.note_slots(&[(last * 4096 + 16, page_meta(last, round + 3))])
            .unwrap();
        folding_commit(
            &log,
            PAGES as u32 + 2 + u32::from(round),
            |_| Ok(true),
            &mut body,
            None,
        )
        .unwrap();
    }
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, policy).unwrap();
    for page in 0..last {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 1),
            "page {page}"
        );
    }
    assert_eq!(
        log.slot(last * 4096 + 16).unwrap(),
        Some(page_meta(last, 14))
    );
}

/// Past the ceiling, where the threshold reaches every segment, reclaim folds
/// sparse records too, so the delta cannot grow without bound. Below it the
/// same records are carried.
#[test]
fn past_the_ceiling_everything_folds() {
    for (policy, folds) in [
        (CARRY, false),
        (
            Policy {
                reclaim_slope: u64::MAX,
                ..CARRY
            },
            true,
        ),
    ] {
        let d = dir();
        let mut body = MemBody::new((PAGES * 4096) as usize);
        let log = fill_without_folding(&d, policy, &mut body);
        log.note_slots(&[(16, page_meta(0, 2))]).unwrap();
        folding_commit(
            &log,
            PAGES as u32 + 1,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .unwrap();
        assert!(!delta_dir(&d).join(segment_name(1)).exists(), "{policy:?}");
        assert_eq!(log.stats().fold_records > 0, folds, "{policy:?}");
    }
}

/// Reclaim folds every record it may, one round of `fold_round_pages` at a
/// time, rather than carrying those past a per-checkpoint cap.
#[test]
fn reclaim_folds_in_rounds_and_carries_nothing_it_may_fold() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let policy = Policy {
        fold_round_pages: 1,
        ..SMALL
    };
    let log = fill_without_folding(&d, policy, &mut body);
    let last = PAGES - 1;
    log.note_slots(&[(last * 4096 + 16, page_meta(last, 2))])
        .unwrap();
    let r = reporting_commit(
        &log,
        PAGES as u32 + 1,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX),
    )
    .unwrap();
    // Segment 1 holds pages 0 and 1: page 0 never folds, so it is carried.
    assert_eq!((r.folded, r.carried, r.fold_rounds), (1, 1, 1), "{r:?}");
    assert!(!delta_dir(&d).join(segment_name(1)).exists());
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, policy).unwrap();
    for page in 0..last {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 1)
        );
    }
}

/// A checkpoint that hot-folds many pages over many rounds folds each
/// page's latest note: every page reads its round-2 value, before and
/// after a reopen.
#[test]
fn many_fold_rounds_keep_each_pages_latest_note() {
    const FOLD_PAGES: u64 = 64;
    let d = dir();
    let mut body = spent_body(FOLD_PAGES);
    let policy = Policy {
        fold_round_pages: 8,
        ..Policy::DEFAULT
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    let note = |round: u8| {
        let notes: Vec<(u64, [u8; 8])> = (0..FOLD_PAGES)
            .map(|page| (page * 4096 + 16, page_meta(page, round)))
            .collect();
        log.note_slots(&notes).unwrap();
    };
    note(1);
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    note(2);
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    // Page 0 holds the `spent.body` header and never folds.
    assert_eq!(r.hot_folds, FOLD_PAGES - 1, "{r:?}");
    assert!(r.fold_rounds >= (FOLD_PAGES - 1) / 8, "{r:?}");
    // The fold writes each page's committed record into `spent.body`;
    // round 2's notes are the checkpoint's new record.
    for page in 1..FOLD_PAGES {
        assert_eq!(body.slot(page * 4096 + 16), page_meta(page, 1));
    }
    for page in 0..FOLD_PAGES {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 2)
        );
    }
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, policy).unwrap();
    for page in 0..FOLD_PAGES {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 2)
        );
    }
}

/// A checkpoint merges each touched page's notes over its committed
/// record into one record: it keeps every committed slot, before, between
/// and after the notes, and a note wins over the committed slot it rewrites.
#[test]
fn a_checkpoint_merges_each_page_into_one_record() {
    const COMMITTED: u64 = 20;
    // A committed slot past every note, so the merge copies a tail.
    const TAIL: u64 = COMMITTED + 20;
    fn meta(page: u64, round: u8) -> [u8; 8] {
        let mut m = [0u8; 8];
        m[..4].copy_from_slice(&(page as u32).to_le_bytes());
        m[4] = round;
        m[7] = 1;
        m
    }
    let pages = 200;
    let d = dir();
    let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
    let abs = |page: u64, slot: u64| page * 4096 + 16 + slot * 8;
    let committed: Vec<(u64, [u8; 8])> = (1..=pages)
        .flat_map(|page| {
            (0..COMMITTED)
                .chain([TAIL])
                .map(move |s| (abs(page, s), meta(page, 1)))
        })
        .collect();
    log.note_slots(&committed).unwrap();
    commit(&log, 1, 0, |_| Ok(true));
    let rewrites: Vec<(u64, [u8; 8])> = (1..=pages)
        .flat_map(|page| {
            [
                (abs(page, 3), meta(page, 2)),
                (abs(page, COMMITTED + 7), meta(page, 3)),
            ]
        })
        .collect();
    log.note_slots(&rewrites).unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut MemBody::new(0), None).unwrap();
    assert_eq!(r.sealed_pages, pages, "{r:?}");
    assert!(r.merge_read_bytes > 0, "{r:?}");
    for page in 1..=pages {
        for s in (0..COMMITTED).chain([TAIL]).filter(|&s| s != 3) {
            assert_eq!(log.slot(abs(page, s)).unwrap(), Some(meta(page, 1)));
        }
        assert_eq!(log.slot(abs(page, 3)).unwrap(), Some(meta(page, 2)));
        assert_eq!(
            log.slot(abs(page, COMMITTED + 7)).unwrap(),
            Some(meta(page, 3))
        );
    }
    // Page 1's new record, byte for byte: the committed slots in slot
    // order with the rewritten one replaced, the new slot, then the tail.
    let mut want = 4096u64.to_le_bytes().to_vec();
    want.extend_from_slice(&(COMMITTED as u16 + 2).to_le_bytes());
    for s in 0..COMMITTED {
        want.extend_from_slice(&(2 + s as u16).to_le_bytes());
        want.extend_from_slice(&meta(1, if s == 3 { 2 } else { 1 }));
    }
    want.extend_from_slice(&(2 + COMMITTED as u16 + 7).to_le_bytes());
    want.extend_from_slice(&meta(1, 3));
    want.extend_from_slice(&(2 + TAIL as u16).to_le_bytes());
    want.extend_from_slice(&meta(1, 1));
    let found = std::fs::read_dir(delta_dir(&d))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("seg"))
        .any(|p| {
            let bytes = std::fs::read(p).unwrap();
            bytes.windows(want.len()).any(|w| w == want.as_slice())
        });
    assert!(found, "page 1's merged record is not in a segment");
}

/// A checkpoint whose reclaim carries records alongside the ones it merges
/// keeps every page's slots: touched pages read the new note, untouched
/// ones their committed slots.
#[test]
fn a_checkpoint_that_carries_records_keeps_every_page() {
    const BIG: u64 = 300;
    const PAIRS: u64 = 300;
    let d = dir();
    // Two 10-slot records share a segment; a 100-slot record has its own.
    let policy = Policy {
        segment_cap: 256,
        reclaim_rate: 100,
        ..CARRY
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    let meta = |page: u64, round: u8| {
        let mut m = [0u8; 8];
        m[..4].copy_from_slice(&(page as u32).to_le_bytes());
        m[4] = round;
        m[7] = 1;
        m
    };
    let abs = |page: u64, slot: u64| page * 4096 + 16 + slot * 8;
    let small = BIG + 1..=BIG + 2 * PAIRS;
    let mut first: Vec<(u64, [u8; 8])> = (1..=BIG)
        .flat_map(|page| (0..100).map(move |s| (abs(page, s), meta(page, 1))))
        .collect();
    first.extend(
        small
            .clone()
            .flat_map(|page| (0..10).map(move |s| (abs(page, s), meta(page, 1)))),
    );
    log.note_slots(&first).unwrap();
    commit(&log, 1, 0, |_| Ok(true));
    // Touch every big page and the second page of each small pair: each
    // small segment is left half live, and reclaim carries its other record.
    let touched = |page: u64| page <= BIG || (page - BIG).is_multiple_of(2);
    let touch: Vec<(u64, [u8; 8])> = (1..=BIG + 2 * PAIRS)
        .filter(|&page| touched(page))
        .map(|page| (abs(page, 0), meta(page, 2)))
        .collect();
    log.note_slots(&touch).unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut MemBody::new(0), Some(u32::MAX)).unwrap();
    assert!(r.carried > PAIRS / 2, "{r:?}");
    for page in 1..=BIG + 2 * PAIRS {
        let round = if touched(page) { 2 } else { 1 };
        assert_eq!(log.slot(abs(page, 0)).unwrap(), Some(meta(page, round)));
        assert_eq!(log.slot(abs(page, 9)).unwrap(), Some(meta(page, 1)));
    }
}

/// A page over zeroes folded right after a page over `spent.body` in the
/// same checkpoint holds only its own spends: a slot it never noted still
/// reads zero, not the slot the page before it held.
#[test]
fn a_page_over_zeroes_folds_without_the_page_before_it() {
    let d = dir();
    let mut body = MemBody::new(3 * 4096);
    for slot in body.bytes[4096..2 * 4096].chunks_exact_mut(8) {
        slot.copy_from_slice(&SPENT);
    }
    body.bytes[4096 + 16..4096 + 24].fill(0);
    let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    // Page 2 spends 300 of its 512 slots: past half, so it folds when touched.
    let page2: Vec<(u64, [u8; 8])> = (0..300u64)
        .map(|i| (2 * 4096 + 8 * i, page_meta(2, 1)))
        .collect();
    log.note_slots(&page2).unwrap();
    log.note_slots(&[(4096 + 16, page_meta(1, 1))]).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[
        (4096 + 16, page_meta(1, 2)),
        (2 * 4096 + 8 * 400, page_meta(2, 2)),
    ])
    .unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 2, "{r:?}");
    let never_noted = 2 * 4096 + 8 * 450;
    assert_eq!(body.slot(never_noted), [0u8; 8]);
    assert_eq!(read_through(&log, &body, never_noted), [0u8; 8]);
    assert_eq!(
        read_through(&log, &body, 2 * 4096 + 8 * 299),
        page_meta(2, 1)
    );
    assert_eq!(read_through(&log, &body, 4096 + 24), SPENT);
}

/// The reclaim threshold rises with the delta's share of `spent.body`:
/// a segment under `reclaim_slope ×` that share is reclaimed, a denser one
/// waits.
#[test]
fn the_reclaim_threshold_rises_with_the_delta_share() {
    // Segment 1 holds pages 0 and 1. Rewriting page 0 leaves it half live,
    // and every other segment stays wholly live.
    let rewrite_page_zero = |d: &std::path::Path, policy: Policy, body: &mut MemBody| {
        let log = fill_without_folding(d, policy, body);
        log.note_slots(&[(16, page_meta(0, 2))]).unwrap();
        folding_commit(&log, PAGES as u32 + 1, |_| Ok(true), body, None).unwrap();
        log
    };
    let body_len = PAGES * 4096;
    let probe = rewrite_page_zero(&dir(), CARRY, &mut MemBody::new(body_len as usize));
    let s = probe.stats();
    let share = (s.segment_bytes - s.segments * 4096) * 1000 / body_len;
    drop(probe);
    for (limit, reclaimed) in [(500u64, false), (600, true)] {
        let d = dir();
        let mut body = MemBody::new(body_len as usize);
        let policy = Policy {
            sparse_permille: 0,
            reclaim_slope: limit / share,
            ..CARRY
        };
        let log = rewrite_page_zero(&d, policy, &mut body);
        log.note_slots(&[(16, page_meta(0, 3))]).unwrap();
        folding_commit(
            &log,
            PAGES as u32 + 2,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .unwrap();
        assert_eq!(
            !delta_dir(&d).join(segment_name(1)).exists(),
            reclaimed,
            "threshold {limit}"
        );
        assert!(
            delta_dir(&d).join(segment_name(2)).exists(),
            "threshold {limit}: a wholly live segment waits"
        );
        assert_eq!(log.slot(4096 + 16).unwrap(), Some(page_meta(1, 1)));
    }
}

/// A delta above the threshold shrinks under steady inflow: one checkpoint
/// reclaims `reclaim_rate` times its own record bytes, not just as many as
/// it adds.
#[test]
fn a_delta_above_the_threshold_shrinks_under_steady_inflow() {
    let d = dir();
    let pages = 4 * PAGES;
    let mut body = MemBody::new((pages * 4096) as usize);
    // Four one-slot records to a segment, so one checkpoint's inflow fills
    // one segment.
    let policy = Policy {
        segment_cap: 16 + 4 * 20,
        reclaim_rate: 2,
        ..SMALL
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    for page in 0..2 * PAGES {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        folding_commit(&log, page as u32 + 1, |_| Ok(true), &mut body, None).unwrap();
    }
    let before = log.stats().segment_bytes;
    let mut tip = 2 * PAGES as u32;
    for round in 0..10u64 {
        let inflow: Vec<(u64, [u8; 8])> = (0..4u64)
            .map(|k| {
                let page = 2 * PAGES + 4 * round + k;
                (page * 4096 + 16, page_meta(page, 1))
            })
            .collect();
        log.note_slots(&inflow).unwrap();
        tip += 1;
        folding_commit(&log, tip, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    }
    let after = log.stats().segment_bytes;
    assert!(after < before, "{before} -> {after}");
}

/// Deciding whether a record folds reads no `spent.body` page: the count of
/// filled slots comes from the append. Only a fold reads its page, to
/// compose it.
#[test]
fn the_fold_decision_reads_no_body_page() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let policy = Policy {
        reclaim_rate: 1000,
        ..CARRY
    };
    let log = fill_without_folding(&d, policy, &mut body);
    // Rewriting page 0 leaves segment 1, pages 0 and 1, half live.
    log.note_slots(&[(16, page_meta(0, 2))]).unwrap();
    body.reads.set(0);
    folding_commit(
        &log,
        PAGES as u32 + 1,
        |_| Ok(true),
        &mut body,
        Some(u32::MAX),
    )
    .unwrap();
    assert_eq!(log.stats().fold_records, 0);
    assert_eq!(body.reads.get(), 0, "carried records are decided unread");
    assert!(!delta_dir(&d).join(segment_name(1)).exists());
}

/// A checkpoint reports what it read and wrote, so the operator log shows
/// which step costs the time: the sealed notes, the committed records read
/// back to merge them, and the record and `idx` bytes written.
#[test]
fn a_checkpoint_reports_its_reads_and_writes() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let mut body = MemBody::new(0);
    log.note_slots(&[(16, [1u8; 8]), (24, [2u8; 8]), (4096, [3u8; 8])])
        .unwrap();
    let first = reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!((first.sealed_pages, first.sealed_edges), (2, 3));
    assert_eq!((first.merge_reads, first.merge_read_bytes), (0, 0));
    assert_eq!(first.record_bytes, (10 + 2 * 10) + (10 + 10));
    assert_eq!(first.idx_entries, 2);
    log.note_slots(&[(32, [4u8; 8])]).unwrap();
    let second = reporting_commit(&log, 2, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!((second.sealed_pages, second.sealed_edges), (1, 1));
    assert_eq!((second.merge_reads, second.merge_read_bytes), (1, 30));
    assert_eq!(second.record_bytes, 10 + 3 * 10);
    assert_eq!(second.idx_entries, 1);
}

/// Records one generation wrote side by side are read back in one pread
/// when a later checkpoint touches their pages again.
#[test]
fn a_merge_reads_neighbouring_records_together() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let mut body = MemBody::new(0);
    let pages = [4096u64, 2 * 4096, 3 * 4096];
    log.note_slots(&pages.map(|p| (p + 16, [1u8; 8]))).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&pages.map(|p| (p + 24, [2u8; 8]))).unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!((r.merge_reads, r.merge_read_bytes), (1, 3 * (10 + 10)));
    for p in pages {
        assert_eq!(log.slot(p + 16).unwrap(), Some([1u8; 8]));
        assert_eq!(log.slot(p + 24).unwrap(), Some([2u8; 8]));
    }
}

/// Records are read together only when they touch: a dead record between
/// two live ones splits the read rather than being read.
#[test]
fn a_merge_does_not_read_a_dead_record_between_two_live_ones() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let mut body = MemBody::new(0);
    let pages = [4096u64, 2 * 4096, 3 * 4096];
    log.note_slots(&pages.map(|p| (p + 16, [1u8; 8]))).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(pages[1] + 24, [2u8; 8])]).unwrap();
    reporting_commit(&log, 2, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(pages[0] + 24, [3u8; 8]), (pages[2] + 24, [3u8; 8])])
        .unwrap();
    let r = reporting_commit(&log, 3, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!((r.merge_reads, r.merge_read_bytes), (2, 2 * 20));
}

/// A checkpoint that reclaims reports the segments it read and what became
/// of their live records, matching the lifetime counters.
#[test]
fn a_fold_reports_the_segments_it_read_and_the_pages_it_folded() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    let (mut folded, mut segments, mut read) = (0, 0, 0);
    for page in 0..PAGES {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        let before = log.stats();
        let r = reporting_commit(
            &log,
            page as u32 + 1,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        )
        .unwrap();
        let after = log.stats();
        assert_eq!(r.folded, after.fold_records - before.fold_records);
        assert_eq!(r.record_bytes, after.record_bytes - before.record_bytes);
        if r.folded > 0 {
            assert!(r.reclaim_segments > 0 && r.reclaim_read_bytes > 0, "{r:?}");
        }
        folded += r.folded;
        segments += r.reclaim_segments;
        read += r.reclaim_read_bytes;
    }
    assert!(folded > 0 && segments > 0 && read > 0);
}

/// Reclaim reads only the live records of a victim segment, found from the
/// directory, not the dead bytes around them.
#[test]
fn reclaim_reads_only_the_live_records() {
    const ROOMY: Policy = Policy {
        segment_cap: 6000,
        reclaim_from_segments: 2,
        keep_segments: 1,
        sparse_permille: 999,
        reclaim_slope: 0,
        reclaim_rate: 100,
        fold_round_pages: usize::MAX,
        log_fold_min: 4 << 20,
        open_edge_cap: rbitcoin_store::spend_delta::OPEN_EDGE_CAP,
    };
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let log = SpendPageLog::create_with(&d, ROOMY).unwrap();
    let (big, small) = (4 * 4096u64, 5 * 4096u64);
    let notes: Vec<(u64, [u8; 8])> = (0..500u64).map(|rel| (big + rel * 8, [1u8; 8])).collect();
    log.note_slots(&notes).unwrap();
    log.note_slots(&[(small, [2u8; 8])]).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    // The big page's record moves to a new segment; the first keeps it dead
    // beside the small page's live 20-byte record.
    log.note_slots(&[(big + 500 * 8, [3u8; 8])]).unwrap();
    reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    log.note_slots(&[(6 * 4096, [4u8; 8])]).unwrap();
    let r = reporting_commit(&log, 3, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.reclaim_segments, 1, "{r:?}");
    assert_eq!(r.reclaim_read_bytes, 20, "{r:?}");
    assert_eq!(read_through(&log, &body, small), [2u8; 8]);
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, ROOMY).unwrap();
    assert_eq!(read_through(&log, &body, small), [2u8; 8]);
    assert_eq!(read_through(&log, &body, big + 500 * 8), [3u8; 8]);
}

/// A record this checkpoint rewrites is dead before reclaim reads its
/// segment, so reclaim does not read it.
#[test]
fn reclaim_does_not_read_a_record_this_checkpoint_rewrites() {
    const ROOMY: Policy = Policy {
        segment_cap: 6000,
        reclaim_from_segments: 2,
        keep_segments: 1,
        sparse_permille: 999,
        reclaim_slope: 0,
        reclaim_rate: 100,
        fold_round_pages: usize::MAX,
        log_fold_min: 4 << 20,
        open_edge_cap: rbitcoin_store::spend_delta::OPEN_EDGE_CAP,
    };
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let log = SpendPageLog::create_with(&d, ROOMY).unwrap();
    let (big, small) = (4 * 4096u64, 5 * 4096u64);
    let notes: Vec<(u64, [u8; 8])> = (0..500u64).map(|rel| (big + rel * 8, [1u8; 8])).collect();
    log.note_slots(&notes).unwrap();
    log.note_slots(&[(small, [2u8; 8])]).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    log.note_slots(&[(6 * 4096, [4u8; 8])]).unwrap();
    reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    // The big page's record in the first segment dies with this checkpoint.
    log.note_slots(&[(big + 500 * 8, [3u8; 8])]).unwrap();
    let r = reporting_commit(&log, 3, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.reclaim_segments, 1, "{r:?}");
    assert_eq!(r.reclaim_read_bytes, 20, "{r:?}");
    assert_eq!(read_through(&log, &body, small), [2u8; 8]);
    assert_eq!(read_through(&log, &body, big), [1u8; 8]);
    assert_eq!(read_through(&log, &body, big + 500 * 8), [3u8; 8]);
}

#[cfg(debug_assertions)]
/// Every file in `spent.delta/` and its length.
fn delta_files(d: &std::path::Path) -> std::collections::HashMap<PathBuf, u64> {
    std::fs::read_dir(delta_dir(d))
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            (e.path(), e.metadata().unwrap().len())
        })
        .collect()
}

#[cfg(debug_assertions)]
/// Zero the 4 KiB blocks of `path` that `[offset, offset + len)` touches,
/// as far as the file reaches.
fn tear_blocks(path: &std::path::Path, offset: u64, len: u64) {
    use std::io::{Seek, SeekFrom, Write};
    let file_len = std::fs::metadata(path).unwrap().len();
    let first = offset / 4096 * 4096;
    let end = (offset + len).div_ceil(4096) * 4096;
    let end = end.min(file_len);
    if first >= end {
        return;
    }
    let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.seek(SeekFrom::Start(first)).unwrap();
    f.write_all(&vec![0u8; (end - first) as usize]).unwrap();
}

#[cfg(debug_assertions)]
/// Run `f`, which ends in one commit, then crash before that commit's
/// rename and tear every 4 KiB block a write touched in a file that existed
/// before `f` to zeroes: the torn page of finding 107. A length cut that
/// ends mid-block tears that block too. Files `f` created are not torn; no
/// commit names them.
fn crash_tearing_writes(d: &std::path::Path, f: impl FnOnce()) {
    // A directory where `commit` was makes its rename fail: everything up to
    // the rename runs, nothing after it.
    tear_writes(d, true, f);
}

#[cfg(debug_assertions)]
/// [`crash_tearing_writes`] for open's own publishes, which must read the
/// commit first: `f` runs to the end and the saved commit is put back. Only
/// for publishes that unlink nothing.
fn tear_open_writes(d: &std::path::Path, f: impl FnOnce()) {
    tear_writes(d, false, f);
}

#[cfg(debug_assertions)]
fn tear_writes(d: &std::path::Path, block_rename: bool, f: impl FnOnce()) {
    use rbitcoin_store::file::{arm_write_trace, take_write_trace, TracedWrite};
    let saved = std::fs::read(commit_path(d)).unwrap();
    let before = delta_files(d);
    if block_rename {
        std::fs::remove_file(commit_path(d)).unwrap();
        std::fs::create_dir(commit_path(d)).unwrap();
    }
    arm_write_trace();
    f();
    let trace = take_write_trace();
    if block_rename {
        std::fs::remove_dir(commit_path(d)).unwrap();
    }
    std::fs::write(commit_path(d), saved).unwrap();
    for w in trace {
        match w {
            TracedWrite::Write { path, offset, len } if before.contains_key(&path) => {
                tear_blocks(&path, offset, len);
            }
            TracedWrite::SetLen { path, len } if before.contains_key(&path) && len % 4096 != 0 => {
                tear_blocks(&path, len, 1);
            }
            _ => {}
        }
    }
}

/// A checkpoint that crashes before its rename, with every block it wrote
/// torn to zeroes, leaves every committed slot readable: no write touches a
/// block holding bytes the commit names.
#[cfg(debug_assertions)]
#[test]
fn a_torn_publish_keeps_every_committed_slot() {
    let d = dir();
    let pub_len = 8 * 4096u64;
    let old: Vec<(u64, [u8; 8])> = (0..8u64)
        .map(|p| (p * 4096 + 16, [p as u8 + 1; 8]))
        .collect();
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&old).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
        crash_tearing_writes(&d, || {
            log.note_slots(&[(3 * 4096 + 24, [9u8; 8]), (4 * 4096 + 16, [9u8; 8])])
                .unwrap();
            assert!(try_commit(&log, 2, |_| Ok(true)).is_err());
        });
    }
    let log = reload(&d, pub_len).unwrap();
    for &(abs, meta) in &old {
        assert_eq!(log.slot(abs).unwrap(), Some(meta), "slot {abs}");
    }
    assert_eq!(log.slot(3 * 4096 + 24).unwrap(), Some([0u8; 8]));
}

/// The same across a segment roll: the segment the roll closes keeps its
/// committed records.
#[cfg(debug_assertions)]
#[test]
fn a_torn_roll_keeps_every_committed_slot() {
    let d = dir();
    let pub_len = 64 * 4096u64;
    let mut model = std::collections::BTreeMap::new();
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    for page in 0..6u64 {
        let note = (page * 4096 + 16, page_meta(page, 1));
        log.note_slots(&[note]).unwrap();
        model.insert(note.0, note.1);
        commit(&log, page as u32 + 1, pub_len, |_| Ok(true));
    }
    crash_tearing_writes(&d, || {
        log.note_slots(&[(10 * 4096 + 16, [7u8; 8]), (11 * 4096 + 16, [7u8; 8])])
            .unwrap();
        assert!(try_commit(&log, 7, |_| Ok(true)).is_err());
    });
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    for (&abs, &meta) in &model {
        assert_eq!(log.slot(abs).unwrap(), Some(meta), "slot {abs}");
    }
}

/// The same for a checkpoint that folds: the fold file guards
/// `spent.body`, and the delta's own files keep the committed records.
#[cfg(debug_assertions)]
#[test]
fn a_torn_fold_checkpoint_keeps_every_committed_slot() {
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    let log = churn(&d, &mut body, Some(u32::MAX));
    crash_tearing_writes(&d, || {
        log.note_slots(&[(16, [9u8; 8])]).unwrap();
        let r = folding_commit(
            &log,
            PAGES as u32 + 1,
            |_| Ok(true),
            &mut body,
            Some(u32::MAX),
        );
        assert!(r.is_err());
    });
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, SMALL).unwrap();
    for page in 0..PAGES {
        assert_eq!(
            read_through(&log, &body, page * 4096 + 16),
            page_meta(page, 1),
            "page {page}"
        );
    }
}

/// Open's own publishes, the clip of a cut body and the commit of the
/// counted tail, crash the same way and open again to the same slots.
#[cfg(debug_assertions)]
#[test]
fn a_torn_open_publish_keeps_every_committed_slot() {
    let d = dir();
    let pub_len = 8 * 4096u64;
    let old: Vec<(u64, [u8; 8])> = (0..8u64)
        .map(|p| (p * 4096 + 16, [p as u8 + 1; 8]))
        .collect();
    {
        let log = SpendPageLog::create(&d).unwrap();
        log.note_slots(&old).unwrap();
        log.note_slots(&[(7 * 4096 + 2048, [9u8; 8])]).unwrap();
        commit(&log, 1, pub_len, |_| Ok(true));
    }
    let cut = 7 * 4096 + 1024;
    tear_open_writes(&d, || {
        drop(reload(&d, cut).unwrap());
    });
    let log = reload(&d, cut).unwrap();
    for &(abs, meta) in &old {
        assert_eq!(log.slot(abs).unwrap(), Some(meta), "slot {abs}");
    }
    assert_eq!(log.slot(7 * 4096 + 2048).unwrap(), Some([0u8; 8]));
}

/// Every checkpoint folds the directory log into a new base.
const REBASE: Policy = Policy {
    log_fold_min: 0,
    ..SMALL
};

/// Once the log outgrows its base, a checkpoint writes the whole directory
/// to a new base and an empty log, and the old pair goes after the commit.
#[test]
fn a_long_log_folds_into_a_new_base() {
    let d = dir();
    let pub_len = 16 * 4096u64;
    let log = SpendPageLog::create_with(&d, REBASE).unwrap();
    for page in 0..6u64 {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        commit(&log, page as u32 + 1, pub_len, |_| Ok(true));
    }
    let mut names: Vec<String> = std::fs::read_dir(delta_dir(&d))
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("idx.") || n.starts_with("log."))
        .collect();
    names.sort();
    assert_eq!(names, vec![idx_name(6), log_name(6)]);
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    for page in 0..6u64 {
        assert_eq!(
            log.slot(page * 4096 + 16).unwrap(),
            Some(page_meta(page, 1))
        );
    }
}

/// A new base whose commit is never renamed is removed at open, and the old
/// base and log, untouched, still hold every committed entry.
#[cfg(debug_assertions)]
#[test]
fn a_torn_rebase_keeps_every_committed_slot() {
    let d = dir();
    let pub_len = 16 * 4096u64;
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    for page in 0..6u64 {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        commit(&log, page as u32 + 1, pub_len, |_| Ok(true));
    }
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut MemBody::new(pub_len as usize), REBASE).unwrap();
    crash_tearing_writes(&d, || {
        log.note_slots(&[(7 * 4096 + 16, [9u8; 8])]).unwrap();
        assert!(try_commit(&log, 7, |_| Ok(true)).is_err());
    });
    assert!(delta_dir(&d).join(idx_name(7)).exists(), "the rebase ran");
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    assert!(!delta_dir(&d).join(idx_name(7)).exists());
    for page in 0..6u64 {
        assert_eq!(
            log.slot(page * 4096 + 16).unwrap(),
            Some(page_meta(page, 1))
        );
    }
    assert_eq!(log.slot(7 * 4096 + 16).unwrap(), Some([0u8; 8]));
}

/// Notes on `count` slots of `page`, from slot 0 (slot 2 on page 0).
fn dense_notes(page: u64, count: u64, round: u8) -> Vec<(u64, [u8; 8])> {
    let first = if page == 0 { 2 } else { 0 };
    (first..first + count)
        .map(|rel| (page * 4096 + rel * 8, page_meta(page, round)))
        .collect()
}

/// A page whose committed record already spends half its unset slots folds
/// when a checkpoint touches it again, instead of being rewritten whole: the
/// new record holds only the notes since.
#[test]
fn a_dense_page_folds_when_a_checkpoint_touches_it() {
    let d = dir();
    let mut body = MemBody::new(4 * 4096);
    let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    log.note_slots(&dense_notes(1, 300, 1)).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(4096 + 400 * 8, [9u8; 8])]).unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 1, "{r:?}");
    assert_eq!(r.hot_fold_record_bytes, 10 + 300 * 10);
    assert_eq!(r.record_bytes, 10 + 10, "only the new note is rewritten");
    for (abs, meta) in dense_notes(1, 300, 1) {
        assert_eq!(body.slot(abs), meta);
        assert_eq!(read_through(&log, &body, abs), meta);
    }
    assert_eq!(log.slot(4096 + 400 * 8).unwrap(), Some([9u8; 8]));
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    assert_eq!(read_through(&log, &body, 4096 + 8), page_meta(1, 1));
    assert_eq!(read_through(&log, &body, 4096 + 400 * 8), [9u8; 8]);
}

/// A dense touched page stays in the delta when its record's segment holds
/// a generation above the durable marker, or the page is not wholly below
/// the published end, and the report says which.
#[test]
fn a_dense_page_stays_until_its_record_is_durable_and_below_the_end() {
    for (durable, len, gen_kept, tail_kept) in [
        (Some(0), 4 * 4096, 1, 0),
        (Some(u32::MAX), 4096 + 2048, 0, 1),
    ] {
        let d = dir();
        let mut body = MemBody::new(len);
        let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
        log.note_slots(&dense_notes(1, 300, 1)).unwrap();
        reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
        log.note_slots(&[(4096 + 400 * 8, [9u8; 8])]).unwrap();
        let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, durable).unwrap();
        assert_eq!(r.hot_folds, 0, "{r:?}");
        assert_eq!(
            (r.dense_kept_gen, r.dense_kept_tail),
            (gen_kept, tail_kept),
            "{r:?}"
        );
        assert_eq!(r.record_bytes, 10 + 301 * 10);
        assert_eq!(log.slot(4096 + 8).unwrap(), Some(page_meta(1, 1)));
    }
}

/// A publish that fails after its fold synced `spent.body` keeps the page
/// readable, and a retry without a marker, which folds nothing, writes the
/// whole record again over the same values.
#[test]
fn a_failed_publish_after_a_touch_fold_reads_the_same_on_retry() {
    let d = dir();
    let mut body = MemBody::new(4 * 4096);
    let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
    log.note_slots(&dense_notes(1, 300, 1)).unwrap();
    reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    let saved = std::fs::read(commit_path(&d)).unwrap();
    std::fs::remove_file(commit_path(&d)).unwrap();
    std::fs::create_dir(commit_path(&d)).unwrap();
    log.note_slots(&[(4096 + 400 * 8, [9u8; 8])]).unwrap();
    assert!(reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).is_err());
    assert_eq!(
        body.slot(4096 + 8),
        page_meta(1, 1),
        "the fold synced the body"
    );
    std::fs::remove_dir(commit_path(&d)).unwrap();
    std::fs::write(commit_path(&d), saved).unwrap();
    for (abs, meta) in dense_notes(1, 300, 1) {
        assert_eq!(read_through(&log, &body, abs), meta);
    }
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!(r.hot_folds, 0);
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    for (abs, meta) in dense_notes(1, 300, 1) {
        assert_eq!(read_through(&log, &body, abs), meta);
    }
    assert_eq!(read_through(&log, &body, 4096 + 400 * 8), [9u8; 8]);
}

/// Folds run in rounds of `fold_round_pages`: each round writes the fold
/// file, copies its pages and syncs `spent.body`, so a checkpoint folds every
/// page it may with a bounded fold file.
#[test]
fn folds_run_in_rounds() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let policy = Policy {
        fold_round_pages: 2,
        ..Policy::DEFAULT
    };
    let log = SpendPageLog::create_with(&d, policy).unwrap();
    for page in 1..6 {
        log.note_slots(&dense_notes(page, 300, 1)).unwrap();
    }
    reporting_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    for page in 1..6 {
        log.note_slots(&[(page * 4096 + 400 * 8, [9u8; 8])])
            .unwrap();
    }
    let syncs = body.syncs;
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 5, "{r:?}");
    assert_eq!(r.fold_rounds, 3, "{r:?}");
    assert_eq!(body.syncs - syncs, 3);
    for page in 1..6 {
        assert_eq!(body.slot(page * 4096 + 8), page_meta(page, 1));
        assert_eq!(read_through(&log, &body, page * 4096 + 400 * 8), [9u8; 8]);
    }
}

/// Distinct open pages do not ask for an early checkpoint: a record holds
/// only its page's entries, so pages cost what their edges cost, and the
/// edge cap is the one early trigger.
#[test]
fn many_open_pages_with_few_edges_do_not_ask_for_a_checkpoint() {
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let notes: Vec<(u64, [u8; 8])> = (0..70_000u64)
        .map(|page| (page * 4096 + 16, [1u8; 8]))
        .collect();
    log.note_slots(&notes).unwrap();
    assert!(!log.checkpoint_early());
    log.relax_pace();
    assert!(!log.checkpoint_early());
}

/// The lifetime counters count the directory bytes actually written: a log
/// batch is its 16-byte header and 16 bytes per entry, and a new base is its
/// 12 bytes per page.
#[test]
fn written_counts_the_directory_bytes() {
    let d = dir();
    let log = SpendPageLog::create_with(&d, SMALL).unwrap();
    log.note_slots(&[(16, [1u8; 8]), (4096 + 16, [2u8; 8])])
        .unwrap();
    commit(&log, 1, 2 * 4096, |_| Ok(true));
    let s = log.stats();
    assert_eq!(s.idx_bytes, 16 + 2 * 16);
    assert_eq!(s.written(), s.record_bytes + s.idx_bytes);
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut MemBody::new(4 * 4096), REBASE).unwrap();
    log.note_slots(&[(2 * 4096 + 16, [3u8; 8])]).unwrap();
    commit(&log, 2, 4 * 4096, |_| Ok(true));
    // The new base holds the 3 pages the directory reaches, 12 bytes each.
    assert_eq!(log.stats().idx_bytes, 16 + 2 * 16 + 3 * 12);
}

fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).unwrap();
    for e in std::fs::read_dir(from).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_dir() {
            copy_dir(&e.path(), &to.join(e.file_name()));
        } else {
            std::fs::copy(e.path(), to.join(e.file_name())).unwrap();
        }
    }
}

/// A damaged directory refuses open; it never reads as pages without a
/// record. Each case damages a copy of one store whose directory is a base
/// with pages and a log of two batches.
#[test]
fn a_damaged_directory_refuses_open() {
    use std::io::{Seek, SeekFrom, Write};
    let d = dir();
    let pub_len = 16 * 4096u64;
    let log = SpendPageLog::create_with(&d, REBASE).unwrap();
    log.note_slots(&[(16, [1u8; 8])]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    drop(log);
    let log = SpendPageLog::load_with(&d, &mut MemBody::new(4096), SMALL).unwrap();
    for page in 1..3u64 {
        log.note_slots(&[(page * 4096 + 16, page_meta(page, 1))])
            .unwrap();
        commit(&log, page as u32 + 1, pub_len, |_| Ok(true));
    }
    drop(log);
    assert!(reload(&d, pub_len).is_ok());
    let base = idx_name(1);
    let journal = log_name(1);
    let write_at = |path: PathBuf, at: u64, bytes: &[u8]| {
        let mut f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        f.seek(SeekFrom::Start(at)).unwrap();
        f.write_all(bytes).unwrap();
    };
    let cut = |path: PathBuf, len: u64| {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(len)
            .unwrap();
    };
    type Damage<'a> = Box<dyn Fn(&std::path::Path) + 'a>;
    let cases: Vec<(&str, Damage)> = vec![
        (
            "a zeroed batch",
            Box::new(|d| write_at(d.join(&journal), 2 * 4096, &[0u8; 4096])),
        ),
        (
            "a log cut short",
            Box::new(|d| cut(d.join(&journal), 2 * 4096 + 8)),
        ),
        ("a base cut short", Box::new(|d| cut(d.join(&base), 16))),
        (
            "a generation past the commit",
            Box::new(|d| write_at(d.join(&journal), 2 * 4096, &99u32.to_le_bytes())),
        ),
        (
            "a flipped entry byte the checksum alone catches",
            Box::new(|d| write_at(d.join(&journal), 4096 + 16 + 8, &[0x01])),
        ),
        (
            "a page past the directory",
            Box::new(|d| write_at(d.join(&journal), 4096 + 16, &999u32.to_le_bytes())),
        ),
    ];
    for (what, damage) in cases {
        let copy = dir();
        copy_dir(&d, &copy);
        damage(&delta_dir(&copy));
        assert!(reload(&copy, pub_len).is_err(), "{what}");
    }
}

/// No publish writes the first block of a segment that existed before it:
/// that block holds the segment's header, which the commit names even
/// while the segment holds no record.
#[cfg(debug_assertions)]
#[test]
fn a_publish_never_writes_a_segment_header_block() {
    use rbitcoin_store::file::{arm_write_trace, take_write_trace, TracedWrite};
    let d = dir();
    let log = SpendPageLog::create(&d).unwrap();
    let before = delta_files(&d);
    arm_write_trace();
    log.note_slots(&[(5 * 4096 + 16, [9u8; 8])]).unwrap();
    commit(&log, 1, 8 * 4096, |_| Ok(true));
    for w in take_write_trace() {
        if let TracedWrite::Write { path, offset, .. } = w {
            if before.contains_key(&path) && path.to_string_lossy().contains("seg.") {
                assert!(offset >= 4096, "{} written at {offset}", path.display());
            }
        }
    }
}

/// A commit rename that landed before an error, as a failed directory sync
/// leaves it, is what the next generation builds on: a crash during that
/// generation keeps every slot the landed commit holds.
#[cfg(debug_assertions)]
#[test]
fn a_commit_that_landed_before_an_error_is_built_on() {
    let d = dir();
    let pub_len = 8 * 4096u64;
    let a = (4096 + 16, [1u8; 8]);
    let b = (2 * 4096 + 16, [2u8; 8]);
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[a]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    log.note_slots(&[b]).unwrap();
    rbitcoin_store::spend_delta::fail_after_next_commit_rename(&d);
    assert!(try_commit(&log, 2, |_| Ok(true)).is_err());
    crash_tearing_writes(&d, || {
        log.note_slots(&[(3 * 4096 + 16, [3u8; 8])]).unwrap();
        assert!(try_commit(&log, 3, |_| Ok(true)).is_err());
    });
    drop(log);
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(a.0).unwrap(), Some(a.1));
    assert_eq!(log.slot(b.0).unwrap(), Some(b.1));
}

/// A rename that lands before an error may not be durable, so the old base
/// and log stay until the next open: if the old commit is the one that
/// survives, it still finds them.
#[cfg(debug_assertions)]
#[test]
fn a_landed_rebase_keeps_the_old_base_until_its_rename_is_durable() {
    let d = dir();
    let pub_len = 8 * 4096u64;
    let a = (4096 + 16, [1u8; 8]);
    let log = SpendPageLog::create_with(&d, REBASE).unwrap();
    log.note_slots(&[a]).unwrap();
    commit(&log, 1, pub_len, |_| Ok(true));
    let old = std::fs::read(commit_path(&d)).unwrap();
    log.note_slots(&[(2 * 4096 + 16, [2u8; 8])]).unwrap();
    rbitcoin_store::spend_delta::fail_after_next_commit_rename(&d);
    assert!(try_commit(&log, 2, |_| Ok(true)).is_err());
    drop(log);
    std::fs::write(commit_path(&d), old).unwrap();
    let log = reload(&d, pub_len).unwrap();
    assert_eq!(log.slot(a.0).unwrap(), Some(a.1));
}

/// An earlier build's commit refuses open before its `fold` file touches
/// `spent.body`.
#[test]
fn an_earlier_build_refuses_before_copying_its_fold() {
    let d = dir();
    drop(SpendPageLog::create(&d).unwrap());
    let mut old = std::fs::read(commit_path(&d)).unwrap();
    old[6..8].copy_from_slice(&3u16.to_le_bytes());
    old.truncate(128);
    std::fs::write(commit_path(&d), old).unwrap();
    let mut fold = Vec::new();
    fold.extend_from_slice(&rbitcoin_primitives::STORE_MAGIC);
    fold.extend_from_slice(&rbitcoin_primitives::SCHEMA_VERSION.to_le_bytes());
    fold.extend_from_slice(&0u16.to_le_bytes());
    fold.extend_from_slice(&1u32.to_le_bytes());
    fold.extend_from_slice(&4096u64.to_le_bytes());
    fold.extend_from_slice(&[0x5au8; 4096]);
    std::fs::write(
        delta_dir(&d).join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME),
        fold,
    )
    .unwrap();
    let mut body = MemBody::new(2 * 4096);
    let err = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT)
        .err()
        .expect("layout 3 refuses");
    assert!(err.to_string().contains("wipe datadir"), "{err}");
    assert!(body.bytes.iter().all(|&b| b == 0), "the fold was copied");
}

/// A spender that was best chain when its note was sealed keeps its slot
/// through the publish, even if a disconnect makes it look gone while the
/// checkpoint merges: a reconnect before the install sees the sealed note
/// and writes nothing new.
#[test]
fn a_sealed_spend_is_not_zeroed_by_a_reorg_during_the_merge() {
    let d = dir();
    let s = (4096 + 16, [7u8; 8]);
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&[s]).unwrap();
    let frozen = std::cell::Cell::new(false);
    log.checkpoint(
        |l| {
            l.freeze_open(1, Some(|_: &[u8; 8]| Ok(!frozen.get())))?;
            frozen.set(true);
            Ok(())
        },
        &mut MemBody::new(0),
        None,
    )
    .unwrap();
    assert_eq!(log.slot(s.0).unwrap(), Some(s.1));
}

/// A fold sets its page's base count from the image it writes, so the page
/// folds again once its delta spends half of what is left: 256 spends fold a
/// page no fold wrote, then 128 more fold its remaining 256, where 127 would
/// not. Each fold happens when a checkpoint touches the page.
#[test]
fn a_folded_page_folds_again_at_half_what_is_left() {
    for (more, folds) in [(126u64, false), (127, true)] {
        let d = dir();
        let mut body = MemBody::new((PAGES * 4096) as usize);
        let at = |rel: u64| 4096 + rel * 8;
        let meta = |rel: u64, round: u8| [rel as u8, round, 0, 0, 0, 0, 0, 1];
        let log = SpendPageLog::create_with(&d, Policy::DEFAULT).unwrap();
        let notes: Vec<(u64, [u8; 8])> = (0..256u64).map(|rel| (at(rel), meta(rel, 1))).collect();
        log.note_slots(&notes).unwrap();
        folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
        // A touch folds the 256: half the 512 unset slots.
        log.note_slots(&[(at(300), meta(300, 2))]).unwrap();
        let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
        assert_eq!(r.hot_folds, 1, "{r:?}");
        assert_eq!(log.slot(at(0)).unwrap(), None, "the first 256 fold");
        // The record now holds 1 + `more` spends over 256 unset slots.
        let notes: Vec<(u64, [u8; 8])> = (301..301 + more)
            .map(|rel| (at(rel), meta(rel, 3)))
            .collect();
        log.note_slots(&notes).unwrap();
        folding_commit(&log, 3, |_| Ok(true), &mut body, None).unwrap();
        log.note_slots(&[(at(450), meta(450, 4))]).unwrap();
        let r = reporting_commit(&log, 4, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
        assert_eq!(r.hot_folds == 1, folds, "{more} more: {r:?}");
        assert_eq!(log.slot(at(301)).unwrap().is_none(), folds, "{more} more");
        assert_eq!(read_through(&log, &body, at(301)), meta(301, 3));
        assert_eq!(read_through(&log, &body, at(0)), meta(0, 1));
    }
}

/// 256 notes on page `page`: enough to fold a page no fold wrote.
fn half_page(page: u64, round: u8) -> Vec<(u64, [u8; 8])> {
    (0..256u64)
        .map(|rel| (page * 4096 + rel * 8, [rel as u8, round, 0, 0, 0, 0, 0, 1]))
        .collect()
}

/// A fold onto a page no fold wrote composes it over zeroes: bytes a
/// crashed fold left there, which no commit names, never come back as
/// spends.
#[test]
fn a_fold_over_zeroes_never_reads_the_body_page() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let stale = 2 * 4096 + 400 * 8;
    body.bytes[stale as usize..stale as usize + 8].copy_from_slice(&[0xa5; 8]);
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&half_page(2, 1)).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    assert_eq!(log.slot(stale).unwrap(), Some([0u8; 8]), "unread before");
    log.note_slots(&[(2 * 4096 + 300 * 8, [9u8; 8])]).unwrap();
    body.reads.set(0);
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 1, "{r:?}");
    assert_eq!(body.reads.get(), 0, "the page is composed over zeroes");
    assert_eq!(body.slot(stale), [0u8; 8], "the fold wrote the whole page");
    assert_eq!(log.slot(stale).unwrap(), None);
    assert!(!delta_dir(&d)
        .join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME)
        .exists());
}

/// A fold onto a page no fold wrote goes straight to `spent.body`: until its
/// commit lands nothing reads that page there, so a crash that tears it
/// loses no slot.
#[test]
fn a_torn_fold_over_zeroes_loses_nothing() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let notes = half_page(2, 1);
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&notes).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    crash_before_rename(&d, || {
        log.note_slots(&[(2 * 4096 + 300 * 8, [9u8; 8])]).unwrap();
        folding_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    });
    drop(log);
    assert_ne!(body.slot(2 * 4096), [0u8; 8], "the fold reached spent.body");
    body.bytes[2 * 4096..3 * 4096].fill(0);
    let log = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    for &(abs, meta) in &notes {
        assert_eq!(read_through(&log, &body, abs), meta, "slot {abs}");
    }
    assert_eq!(log.slot(2 * 4096 + 300 * 8).unwrap(), Some([0u8; 8]));
}

/// A page a fold wrote that a cut `spent.body` now straddles becomes a
/// record over zeroes of its slots below the cut: its body bytes past the
/// cut belong to creates that will take those offsets again.
#[test]
fn a_cut_through_a_folded_page_keeps_only_the_slots_below_it() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&half_page(2, 1)).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(5 * 4096, [9u8; 8])]).unwrap();
    folding_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    log.note_slots(&[(2 * 4096 + 8, [9u8; 8])]).unwrap();
    let r = reporting_commit(&log, 3, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 1, "{r:?}");
    assert_eq!(
        log.slot(2 * 4096 + 16).unwrap(),
        None,
        "page 2 reads the body"
    );
    drop(log);
    let cut = 2 * 4096 + 100 * 8;
    body.bytes.truncate(cut as usize);
    let log = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    assert_eq!(
        log.slot(2 * 4096 + 16).unwrap(),
        Some([2, 1, 0, 0, 0, 0, 0, 1])
    );
    assert_eq!(log.slot(2 * 4096 + 8).unwrap(), Some([9u8; 8]));
    // Past the cut, the page reads zero again once creates take it.
    drop(log);
    body.bytes.resize(8 * 4096, 0);
    let log = SpendPageLog::load_with(&d, &mut body, Policy::DEFAULT).unwrap();
    assert_eq!(log.slot(2 * 4096 + 200 * 8).unwrap(), Some([0u8; 8]));
}

/// Page 0 never folds: its first block holds the `spent.body` header, which
/// every Class A publish rewrites. A dense record there stays in the delta.
#[test]
fn page_zero_never_folds() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    let notes: Vec<(u64, [u8; 8])> = (2..300u64)
        .map(|rel| (rel * 8, [rel as u8, 1, 0, 0, 0, 0, 0, 1]))
        .collect();
    let log = SpendPageLog::create(&d).unwrap();
    log.note_slots(&notes).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(400 * 8, [9u8; 8])]).unwrap();
    let r = reporting_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert_eq!(r.hot_folds, 0, "{r:?}");
    assert!(body.bytes.iter().all(|&b| b == 0));
    for &(abs, meta) in &notes {
        assert_eq!(log.slot(abs).unwrap(), Some(meta));
    }
}

/// A `fold` file that cannot be removed fails the checkpoint after its
/// commit, so the next checkpoint copies it again, while nothing has
/// touched its pages, and removes it before it folds anything. A stale one
/// left behind could later be copied over a page a direct fold rewrote.
#[cfg(debug_assertions)]
#[test]
fn a_fold_file_that_stays_fails_the_checkpoint() {
    let d = dir();
    let mut body = MemBody::new(8 * 4096);
    for page in 1..8u64 {
        let at = (page * 4096 + 4000) as usize;
        body.bytes[at..at + 8].copy_from_slice(&[0xa5; 8]);
    }
    let log = SpendPageLog::create(&d).unwrap();
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    log.note_slots(&half_page(2, 1)).unwrap();
    folding_commit(&log, 1, |_| Ok(true), &mut body, None).unwrap();
    log.note_slots(&[(2 * 4096 + 300 * 8, [9u8; 8])]).unwrap();
    rbitcoin_store::spend_delta::fail_next_fold_removal(&d);
    assert!(folding_commit(&log, 2, |_| Ok(true), &mut body, Some(u32::MAX)).is_err());
    let fold = delta_dir(&d).join(rbitcoin_store::spend_delta::SPEND_DELTA_FOLD_NAME);
    assert!(fold.exists());
    assert_eq!(log.slot(2 * 4096 + 16).unwrap(), None, "the fold committed");
    log.note_slots(&[(5 * 4096, [7u8; 8])]).unwrap();
    folding_commit(&log, 3, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    assert!(!fold.exists());
    assert_eq!(read_through(&log, &body, 2 * 4096 + 300 * 8), [9u8; 8]);
}

/// Checkpoints that keep failing do not park confirm at the edge cap
/// forever: past `CHECKPOINT_FAILURES_STOP` failures in a row the wait
/// returns an error, and one success clears the count.
#[test]
fn checkpoints_that_keep_failing_stop_the_edge_cap_wait() {
    use rbitcoin_store::spend_delta::CHECKPOINT_FAILURES_STOP;
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    let d = dir();
    let policy = Policy {
        open_edge_cap: EDGE_CAP,
        ..Policy::DEFAULT
    };
    let log = Arc::new(SpendPageLog::create_with(&d, policy).unwrap());
    let slots: Vec<(u64, [u8; 8])> = (0..EDGE_CAP).map(|i| (16 + i * 8, [1u8; 8])).collect();
    log.note_slots(&slots).unwrap();
    log.bind_sync_wake(Arc::new((Mutex::new(false), Condvar::new())));
    let (tx, rx) = mpsc::channel();
    let waiter = Arc::clone(&log);
    std::thread::spawn(move || tx.send(waiter.wait_open_edge_room()).unwrap());
    let fail = |l: &SpendPageLog| {
        l.checkpoint(
            |_| Err(rbitcoin_store::error::StoreError::Corrupt("sync failed")),
            &mut MemBody::new(0),
            None,
        )
        .unwrap_err()
    };
    for _ in 1..CHECKPOINT_FAILURES_STOP {
        fail(&log);
    }
    assert!(rx
        .recv_timeout(std::time::Duration::from_millis(200))
        .is_err());
    fail(&log);
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the wait gives up");
    assert!(got.is_err());
    assert!(log.wait_open_edge_room().is_err(), "still failing");
    folding_commit(&log, 1, |_| Ok(true), &mut MemBody::new(0), None).unwrap();
    assert!(
        log.wait_open_edge_room().is_ok(),
        "a success clears the count"
    );
    fail(&log);
    let (tx, rx) = mpsc::channel();
    let waiter = Arc::clone(&log);
    std::thread::spawn(move || tx.send(waiter.wait_open_edge_room()).unwrap());
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("the published notes hold nothing");
    assert!(got.is_ok());
}

/// A checkpoint that fails after its freeze keeps the notes it sealed. They
/// still count toward the cap, so the parked confirm stays parked instead of
/// noting another cap's worth each retry, and a later success releases it.
/// [`checkpoints_that_keep_failing_stop_the_edge_cap_wait`] covers the stop.
#[test]
fn notes_a_failed_checkpoint_sealed_still_hold_the_edge_cap() {
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    let d = dir();
    let mut body = MemBody::new((PAGES * 4096) as usize);
    for page in 0..PAGES {
        let at = (page * 4096 + 24) as usize;
        body.bytes[at..at + 8].copy_from_slice(&[0xa5u8; 8]);
    }
    body.fail_writes = true;
    let policy = Policy {
        open_edge_cap: EDGE_CAP,
        ..SMALL
    };
    let log = Arc::new(SpendPageLog::create_with(&d, policy).unwrap());
    log.adopt_v29(Vec::new(), 0, &body).unwrap();
    let mut tip = 0;
    let mut failed = 0;
    while failed == 0 {
        tip += 1;
        log.note_slots(&[(u64::from(tip) * 4096 + 16, page_meta(u64::from(tip), 1))])
            .unwrap();
        if folding_commit(&log, tip, |_| Ok(true), &mut body, Some(u32::MAX)).is_err() {
            failed += 1;
        }
    }
    let slots: Vec<(u64, [u8; 8])> = (0..EDGE_CAP).map(|i| (16 + i * 8, [1u8; 8])).collect();
    log.note_slots(&slots).unwrap();
    log.bind_sync_wake(Arc::new((Mutex::new(false), Condvar::new())));
    let (tx, rx) = mpsc::channel();
    let waiter = Arc::clone(&log);
    std::thread::spawn(move || tx.send(waiter.wait_open_edge_room()).unwrap());
    // The first failure seals the cap's notes; the second proves that a
    // retry does not release the waiter by counting the open map again.
    for failed in 1..=2 {
        assert!(folding_commit(&log, tip, |_| Ok(true), &mut body, Some(u32::MAX)).is_err());
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(50))
                .is_err(),
            "parked after {failed} failures"
        );
    }
    // A success publishes the sealed layer, so the parked waiter gets room.
    body.fail_writes = false;
    folding_commit(&log, tip, |_| Ok(true), &mut body, Some(u32::MAX)).unwrap();
    let got = rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("a successful checkpoint releases the waiter");
    assert!(got.is_ok());
}

fn read_exact_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(not(unix))]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "short read",
                ));
            }
            done += n;
        }
        Ok(())
    }
}

fn write_all_at(file: &std::fs::File, bytes: &[u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.write_all_at(bytes, offset)
    }
    #[cfg(not(unix))]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < bytes.len() {
            let n = file.seek_write(&bytes[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "short write",
                ));
            }
            done += n;
        }
        Ok(())
    }
}

/// `spent.body` of a real store, so a fold lands in the file confirm reads.
struct FileBody {
    file: std::fs::File,
    len: u64,
    path: PathBuf,
}

impl SpentBody for FileBody {
    fn published_len(&self) -> u64 {
        self.len
    }

    fn read(&self, abs: u64, buf: &mut [u8]) -> Result<(), rbitcoin_store::error::StoreError> {
        read_exact_at(&self.file, buf, abs)
            .map_err(|e| rbitcoin_store::error::StoreError::io(&self.path, e))
    }

    fn write(&mut self, abs: u64, bytes: &[u8]) -> Result<(), rbitcoin_store::error::StoreError> {
        write_all_at(&self.file, bytes, abs)
            .map_err(|e| rbitcoin_store::error::StoreError::io(&self.path, e))
    }

    fn sync(&mut self) -> Result<(), rbitcoin_store::error::StoreError> {
        self.file
            .sync_all()
            .map_err(|e| rbitcoin_store::error::StoreError::io(&self.path, e))
    }
}

fn spent_meta(tag: u64) -> [u8; 8] {
    rbitcoin_store::tx_table::encode_spent_slot(
        0,
        rbitcoin_primitives::Fk(tag),
        (tag as u32) & 0xffff,
    )
    .unwrap()
}

fn decoded(raw: [u8; 8]) -> rbitcoin_store::tx_table::SpenderSlot {
    let (flags, field, vin) = rbitcoin_store::tx_table::decode_spent_slot(&raw).unwrap();
    (field, flags, vin)
}

/// Confirm's spender-meta batch reads every delta record it still needs in
/// one completion-session submission. A folded page with no record is not in
/// that read. A zero-base miss and an explicit zero stay unspent even when
/// the body bytes would not decode.
#[test]
fn confirm_spend_meta_reads_delta_records_in_one_bulk() {
    use rbitcoin_primitives::Fk;
    use rbitcoin_store::spend_delta::{
        arm_delta_record_owner_probe, delta_record_io, delta_record_owner_held,
        reset_delta_record_io,
    };
    use rbitcoin_store::Store;

    const PAGE: u64 = 4096;
    /// Half the slots on a fresh page: the fold rule's threshold.
    const DENSE: u16 = 256;
    let folded = PAGE;
    let touched = 2 * PAGE;
    let present = 3 * PAGE;
    let hidden = 4 * PAGE;
    let ram_only = 5 * PAGE;
    let published_need = 6 * PAGE;

    let d = dir();
    let (body_path, published) = {
        let s = Store::create_tiny(&d).unwrap();
        // One create whose spent slots cover every page the lookup reads.
        // A bare body append has no locator, and open cuts it back to the header.
        let n_out = 4096u32;
        s.put_tx_full_batch_indexed(
            &[(
                rbitcoin_store::tx_table::TxRecord {
                    txid: [0x11; 32],
                    version: 1,
                    locktime: 0,
                    input_count: 1,
                    output_count: n_out,
                },
                vec![rbitcoin_store::tx_table::InputRecord::coinbase(
                    u32::MAX,
                    vec![0x01],
                    vec![],
                )],
                vec![rbitcoin_store::tx_table::OutputRecord::unspent(0, vec![]); n_out as usize],
            )],
            false,
        )
        .unwrap();
        s.txs.sync_class_a_named(1).unwrap();
        let published = s.txs.spent.body_published_len();
        assert!(
            published >= published_need,
            "published {published} covers the pages under test"
        );
        let path = s.txs.spent.body_file_path().to_path_buf();
        (path, published)
    };

    // One dense record fills a segment. The next rolls. Reclaim, past the
    // ceiling, folds a segment it is not rewriting and drops the one this
    // checkpoint rewrites.
    let policy = Policy {
        segment_cap: 2580,
        reclaim_from_segments: 2,
        keep_segments: 1,
        sparse_permille: 1000,
        reclaim_slope: u64::MAX,
        reclaim_rate: 4,
        fold_round_pages: 64,
        log_fold_min: 4 << 20,
        open_edge_cap: rbitcoin_store::spend_delta::OPEN_EDGE_CAP,
    };
    let mut body = FileBody {
        file: std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&body_path)
            .unwrap(),
        len: published,
        path: body_path.clone(),
    };
    let log = SpendPageLog::load_with(&d, &mut body, policy).unwrap();
    let dense = |page: u64| {
        let notes: Vec<(u64, [u8; 8])> = (0..DENSE)
            .map(|rel| (page + u64::from(rel) * 8, spent_meta(page + u64::from(rel))))
            .collect();
        log.note_slots(&notes).unwrap();
    };
    dense(folded);
    dense(touched);
    log.note_slots(&[(present + 16, spent_meta(present + 2))])
        .unwrap();
    log.note_slots(&[(hidden + 16, [0u8; 8])]).unwrap();
    log.checkpoint(
        |l| l.freeze_open(1, Some(|_: &[u8; 8]| Ok(true))),
        &mut body,
        None,
    )
    .unwrap();

    let meta_new = spent_meta(0x81_0003);
    log.note_slots(&[(touched + 16, [0u8; 8]), (touched + 24, meta_new)])
        .unwrap();
    let folded_round = log
        .checkpoint(
            |l| l.freeze_open(2, Some(|_: &[u8; 8]| Ok(true))),
            &mut body,
            Some(u32::MAX),
        )
        .unwrap();
    assert!(
        folded_round.hot_folds >= 1 && folded_round.folded >= 1,
        "touch-fold and reclaim both ran: {folded_round:?}"
    );
    assert!(
        !delta_dir(&d).join(segment_name(1)).exists(),
        "the reclaimed page has no record file"
    );
    drop(log);
    drop(body);

    // Zeros decode as unspent. Poison the slots a zero answer must not read,
    // so a body peek comes back as None.
    let poison = std::fs::OpenOptions::new()
        .write(true)
        .open(&body_path)
        .unwrap();
    for abs in [hidden + 16, hidden + 24, ram_only + 8] {
        write_all_at(&poison, &[0xff; 8], abs).unwrap();
    }
    drop(poison);

    let s = Store::open_tiny(&d).unwrap();
    assert!(
        s.txs.spent.body_published_len() >= published_need,
        "open kept the spent body"
    );
    let ram = spent_meta(0x12_0004);
    let ram_page = spent_meta(0x20_0001);
    s.note_open_spend_slots(&[(present + 32, ram), (ram_only + 8, ram_page)])
        .unwrap();

    let queries = [
        folded + 16,
        touched + 16,
        touched + 24,
        touched + 32,
        present + 16,
        present + 32,
        hidden + 16,
        hidden + 24,
        ram_only + 8,
    ];
    let unspent = Some((Fk::NULL, 0, 0));
    let expect = [
        Some(decoded(spent_meta(folded + 2))),
        unspent,
        Some(decoded(meta_new)),
        Some(decoded(spent_meta(touched + 4))),
        Some(decoded(spent_meta(present + 2))),
        Some(decoded(ram)),
        unspent,
        unspent,
        Some(decoded(ram_page)),
    ];
    reset_delta_record_io();
    let got = s.get_spender_meta_at_abs_batch(&queries).unwrap();
    assert_eq!(got, expect);
    assert_eq!(
        delta_record_io(),
        (1, 0),
        "three record pages are one bulk submission, not a pread each"
    );
    assert!(
        !delta_record_owner_held(),
        "an unarmed lookup counted segment owners"
    );
    reset_delta_record_io();
    arm_delta_record_owner_probe();
    let armed = s.get_spender_meta_at_abs_batch(&queries).unwrap();
    assert_eq!(armed, expect);
    assert_eq!(
        delta_record_io(),
        (1, 0),
        "three record pages are one bulk submission, not a pread each"
    );
    assert!(
        delta_record_owner_held(),
        "the bulk read dropped the segment file before the pread"
    );
    reset_delta_record_io();
}
