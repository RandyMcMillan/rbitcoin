//! A mark of whole 4 KiB images, and an append that stays off those pages.
use rbitcoin_primitives::{SCHEMA_VERSION, STORE_MAGIC};
use rbitcoin_store::page_guard::{
    append_past_end, arm_page_guard_group, clear_page_guard, restore_page_guard_group,
};
use rbitcoin_store::testutil::TempDir;
use rbitcoin_store::StoreError;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const PAGE: usize = 4096;

fn fill(byte: u8) -> Vec<u8> {
    vec![byte; PAGE]
}

fn write_bytes(path: &Path, off: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap();
    let end = off + bytes.len() as u64;
    if file.metadata().unwrap().len() < end {
        file.set_len(end).unwrap();
    }
    file.seek(SeekFrom::Start(off)).unwrap();
    file.write_all(bytes).unwrap();
}

fn read_bytes(path: &Path, off: u64, n: usize) -> Vec<u8> {
    let mut file = std::fs::File::open(path).unwrap();
    file.seek(SeekFrom::Start(off)).unwrap();
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf).unwrap();
    buf
}

fn open_rw(path: &Path) -> std::fs::File {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .unwrap()
}

#[test]
fn restore_copies_every_armed_page() {
    let dir = TempDir::labeled("page-guard-two").unwrap();
    let live = dir.path().join("live");
    let mark = dir.path().join("guard/live");
    let first = fill(0x41);
    let second = fill(0x42);
    write_bytes(&live, 0, &fill(0x00));
    write_bytes(&live, PAGE as u64, &fill(0x00));
    arm_page_guard_group(
        &mark,
        &[("live", 0, &first), ("live", PAGE as u64, &second)],
    )
    .unwrap();
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&live, 0, PAGE), first);
    assert_eq!(read_bytes(&live, PAGE as u64, PAGE), second);
    assert!(!mark.exists());
}

#[test]
fn missing_mark_leaves_the_mixed_page() {
    let dir = TempDir::labeled("page-guard-absent").unwrap();
    let live = dir.path().join("live");
    let mark = dir.path().join("guard/live");
    let mixed = fill(0x44);
    write_bytes(&live, 0, &mixed);
    std::fs::create_dir_all(mark.parent().unwrap()).unwrap();
    assert!(!restore_page_guard_group(&mark, dir.path()).unwrap());
    let file = open_rw(&live);
    clear_page_guard(&mark, &file, &live).unwrap();
    drop(file);
    assert_eq!(read_bytes(&live, 0, PAGE), mixed);
    assert!(!mark.exists());
}

/// A mark that does not parse, an unopenable schema, and a kind this binary
/// does not read (including the old single-file kind 0) are not absence.
#[test]
fn a_mark_that_does_not_parse_is_not_absence() {
    let dir = TempDir::labeled("page-guard-bad").unwrap();
    let live = dir.path().join("live");
    let mark = dir.path().join("guard/live");
    let mixed = fill(0x55);
    write_bytes(&live, 0, &mixed);
    std::fs::create_dir_all(mark.parent().unwrap()).unwrap();
    std::fs::write(&mark, b"nope").unwrap();
    let err = restore_page_guard_group(&mark, dir.path()).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
    assert_eq!(read_bytes(&live, 0, PAGE), mixed);
    assert!(mark.exists());

    std::fs::remove_file(&mark).unwrap();
    arm_page_guard_group(&mark, &[("live", 0, fill(0x66).as_slice())]).unwrap();
    let good = std::fs::read(&mark).unwrap();
    for (at, bytes) in [(4, 1u16), (6, 0u16), (6, 7u16)] {
        let mut raw = good.clone();
        raw[at..at + 2].copy_from_slice(&bytes.to_le_bytes());
        std::fs::write(&mark, &raw).unwrap();
        let err = restore_page_guard_group(&mark, dir.path()).unwrap_err();
        assert!(
            matches!(err, StoreError::Corrupt(_)),
            "{at}={bytes}: {err:?}"
        );
        assert_eq!(read_bytes(&live, 0, PAGE), mixed);
        assert!(mark.exists());
    }
}

#[test]
fn arm_refuses_a_page_that_is_not_one_image() {
    let dir = TempDir::labeled("page-guard-arm").unwrap();
    let mark = dir.path().join("guard/live");
    let page = fill(0x01);
    let err = arm_page_guard_group(&mark, &[]).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
    let err = arm_page_guard_group(&mark, &[("live", 100, &page)]).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
    let err = arm_page_guard_group(&mark, &[("live", 0, &page[..10])]).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
    let err = arm_page_guard_group(&mark, &[("live", 0, &page), ("live", 0, &page)]).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)));
    assert!(!mark.exists());
}

#[test]
fn arm_refuses_a_bare_name() {
    let dir = TempDir::labeled("page-guard-bare").unwrap();
    let live = dir.path().join("live");
    let page = fill(0x01);
    let bare = Path::new("page-guard-bare-name");
    let err = arm_page_guard_group(bare, &[("live", 0, &page)]).unwrap_err();
    assert!(matches!(err, StoreError::Rejected(_)));
    let file = open_rw(&live);
    let err = clear_page_guard(bare, &file, &live).unwrap_err();
    assert!(matches!(err, StoreError::Rejected(_)));
    let err = restore_page_guard_group(bare, dir.path()).unwrap_err();
    assert!(matches!(err, StoreError::Rejected(_)));
}

const NO_STORE_CHILD: &str = "RBITCOIN_PAGE_GUARD_NO_STORE_CHILD";

/// A mark with no store directory above `guard/` has nowhere to register
/// its pages or sync `guard/`. Arm refuses it before it creates anything.
/// The relative mark resolves against the working directory, so a child
/// process runs it inside a fresh temp directory.
#[test]
fn arm_refuses_a_mark_with_no_store_directory() {
    if std::env::var_os(NO_STORE_CHILD).is_some() {
        let page = fill(0x01);
        let res = arm_page_guard_group(Path::new("guard/live"), &[("live", 0, &page)]);
        assert!(matches!(res, Err(StoreError::Rejected(_))), "{res:?}");
        return;
    }
    let dir = TempDir::labeled("page-guard-no-store-dir").unwrap();
    let out = std::process::Command::new(std::env::current_exe().expect("current_exe"))
        .args([
            "--exact",
            "page_guard::arm_refuses_a_mark_with_no_store_directory",
        ])
        .current_dir(dir.path())
        .env(NO_STORE_CHILD, "1")
        .output()
        .expect("child test");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success() && stdout.contains("1 passed"),
        "child: {stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !dir.path().join("guard").exists(),
        "arm created its guard directory before it refused"
    );
}

/// Open looks for marks only in `guard/` and skips `.tmp` staging names, so
/// arm refuses any other mark path before it creates anything.
#[test]
fn arm_refuses_a_mark_open_would_not_see() {
    let dir = TempDir::labeled("page-guard-namespace").unwrap();
    let page = fill(0x01);
    for mark in ["journal/class_c", "guard/class_c.tmp"] {
        let path = dir.path().join(mark);
        let res = arm_page_guard_group(&path, &[("live", 0, &page)]);
        assert!(
            matches!(res, Err(StoreError::Rejected(_))),
            "{mark}: {res:?}"
        );
        assert!(!path.exists(), "{mark}");
    }
    assert!(!dir.path().join("journal").exists());
}

/// One file has one spelling. `sub//live`, `sub/./live`, and `sub/live/`
/// name the file `sub/live` names, so arm and restore refuse them. A `:`
/// is refused too: on Windows `C:live` joins as a drive prefix.
#[test]
fn a_noncanonical_file_name_is_refused() {
    let dir = TempDir::labeled("page-guard-canon").unwrap();
    let mark = dir.path().join("guard/live");
    let page = fill(0x01);
    for name in ["sub//live", "sub/./live", "sub/live/", "C:live", "c:/live"] {
        let err = arm_page_guard_group(&mark, &[(name, 0, &page)]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{name}: {err:?}");
        assert!(!mark.exists(), "{name}");

        let mut raw = STORE_MAGIC.to_vec();
        raw.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
        raw.extend_from_slice(&1u16.to_le_bytes());
        raw.extend_from_slice(&1u32.to_le_bytes());
        raw.extend_from_slice(&(name.len() as u16).to_le_bytes());
        raw.extend_from_slice(name.as_bytes());
        raw.extend_from_slice(&0u64.to_le_bytes());
        raw.extend_from_slice(&page);
        std::fs::create_dir_all(mark.parent().unwrap()).unwrap();
        std::fs::write(&mark, &raw).unwrap();
        let err = restore_page_guard_group(&mark, dir.path()).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{name}: {err:?}");
        assert!(!dir.path().join("sub").exists(), "{name}");
        std::fs::remove_file(&mark).unwrap();
    }
    std::fs::create_dir_all(dir.path().join("sub")).unwrap();
    arm_page_guard_group(&mark, &[("sub/live", 0, &page)]).unwrap();
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&dir.path().join("sub/live"), 0, PAGE), page);
}

#[test]
fn clear_of_a_missing_mark_syncs_the_directory() {
    let dir = TempDir::labeled("page-guard-clear-missing").unwrap();
    let live = dir.path().join("live");
    let mark = dir.path().join("guard");
    write_bytes(&live, 0, &fill(0x11));
    let file = open_rw(&live);
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        clear_page_guard(&mark, &file, &live).unwrap();
        let dirs = rbitcoin_store::file::take_fsynced_dirs();
        assert!(
            dirs.iter().any(|d| d == dir.path()),
            "a retry after the unlink must still sync the directory, got {dirs:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    clear_page_guard(&mark, &file, &live).unwrap();
}

#[test]
fn arm_syncs_the_store_directory_when_guard_is_new() {
    let dir = TempDir::labeled("page-guard-new-dir").unwrap();
    let mark = dir.path().join("guard").join("mark");
    let page = fill(0x01);
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        arm_page_guard_group(&mark, &[("live", 0, &page)]).unwrap();
        let dirs = rbitcoin_store::file::take_fsynced_dirs();
        assert!(
            dirs.iter().any(|d| d == dir.path()),
            "a new guard directory must be durable in the store directory, got {dirs:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    arm_page_guard_group(&mark, &[("live", 0, &page)]).unwrap();
    assert!(mark.is_file());
}

#[test]
fn arm_syncs_the_store_directory_when_guard_already_exists() {
    let dir = TempDir::labeled("page-guard-old-dir").unwrap();
    let guard = dir.path().join("guard");
    std::fs::create_dir(&guard).unwrap();
    let mark = guard.join("mark");
    let page = fill(0x01);
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        arm_page_guard_group(&mark, &[("live", 0, &page)]).unwrap();
        let dirs = rbitcoin_store::file::take_fsynced_dirs();
        assert!(
            dirs.iter().any(|d| d == dir.path()),
            "a guard directory left by a failed sync must still be synced, got {dirs:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    arm_page_guard_group(&mark, &[("live", 0, &page)]).unwrap();
    assert!(mark.is_file());
}

/// An append is every byte at or past the committed end, including bytes
/// that share the committed tail block: those committed sectors are written
/// back unchanged, so a tear cannot reach them. A byte below the end is not.
#[test]
fn append_past_end_takes_the_tail_block_past_the_end() {
    let dir = TempDir::labeled("page-guard-append").unwrap();
    let path = dir.path().join("live");
    let file = open_rw(&path);
    file.set_len(100).unwrap();

    append_past_end(&file, &path, 100, 100, &[1, 2, 3]).unwrap();
    let err = append_past_end(&file, &path, 100, 99, &[7, 7]).unwrap_err();
    assert!(matches!(err, StoreError::Rejected(_)));
    let err = append_past_end(&file, &path, 4096, 0, &fill(0x01)).unwrap_err();
    assert!(matches!(err, StoreError::Rejected(_)));
    append_past_end(&file, &path, 100, 50, &[]).unwrap();
    append_past_end(&file, &path, 100, 4096, &[9u8; 16]).unwrap();
    drop(file);
    assert_eq!(read_bytes(&path, 100, 3), vec![1, 2, 3]);
    assert_eq!(read_bytes(&path, 4096, 16), vec![9u8; 16]);
    assert_eq!(read_bytes(&path, 0, 100), vec![0u8; 100]);

    let path = dir.path().join("fresh");
    let file = open_rw(&path);
    let image = fill(0x66);
    append_past_end(&file, &path, 0, 0, &image).unwrap();
    drop(file);
    assert_eq!(read_bytes(&path, 0, PAGE), image);
}

#[test]
fn group_restore_names_each_file() {
    let dir = TempDir::labeled("page-guard-group").unwrap();
    let a = dir.path().join("confirmed.body");
    let b = dir.path().join("strong_tx.body");
    let mark = dir.path().join("guard/class_c");
    write_bytes(&a, 0, &fill(0x11));
    write_bytes(&b, 0, &fill(0x22));
    let next_a = fill(0xA1);
    let next_b = fill(0xB2);
    arm_page_guard_group(
        &mark,
        &[
            ("confirmed.body", 0, next_a.as_slice()),
            ("strong_tx.body", 0, next_b.as_slice()),
        ],
    )
    .unwrap();
    write_bytes(&a, 0, &fill(0x00));
    write_bytes(&b, 0, &fill(0x00));
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&a, 0, PAGE), next_a);
    assert_eq!(read_bytes(&b, 0, PAGE), next_b);
    assert!(!mark.exists());
}

#[test]
fn group_arm_replaces_one_file_and_keeps_another() {
    let dir = TempDir::labeled("page-guard-merge").unwrap();
    let head = dir.path().join("header.head");
    let mlt = dir.path().join("header.head.mlt");
    let mark = dir.path().join("guard/header.head");
    let old_head = fill(0x31);
    let kept_page = fill(0x34);
    let kept_mlt = fill(0x32);
    let new_head = fill(0x33);
    arm_page_guard_group(
        &mark,
        &[
            ("header.head", 0, old_head.as_slice()),
            ("header.head", PAGE as u64, kept_page.as_slice()),
            ("header.head.mlt", 0, kept_mlt.as_slice()),
        ],
    )
    .unwrap();
    arm_page_guard_group(&mark, &[("header.head", 0, new_head.as_slice())]).unwrap();
    write_bytes(&head, 0, &fill(0x00));
    write_bytes(&head, PAGE as u64, &fill(0x00));
    write_bytes(&mlt, 0, &fill(0x00));
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&head, 0, PAGE), new_head);
    assert_eq!(read_bytes(&head, PAGE as u64, PAGE), kept_page);
    assert_eq!(read_bytes(&mlt, 0, PAGE), kept_mlt);
}

#[test]
fn clear_drops_only_the_synced_file() {
    let dir = TempDir::labeled("page-guard-clear-one").unwrap();
    let head = dir.path().join("header.head");
    let mlt = dir.path().join("header.head.mlt");
    let mark = dir.path().join("guard/header.head");
    let head_image = fill(0x41);
    let mlt_image = fill(0x42);
    arm_page_guard_group(
        &mark,
        &[
            ("header.head", 0, head_image.as_slice()),
            ("header.head.mlt", 0, mlt_image.as_slice()),
        ],
    )
    .unwrap();
    // The head moves past its marked image, so a restore that still held
    // the head record would show up here.
    let head_written = fill(0x51);
    write_bytes(&head, 0, &head_written);
    write_bytes(&mlt, 0, &fill(0x00));
    let head_file = open_rw(&head);
    clear_page_guard(&mark, &head_file, &head).unwrap();
    drop(head_file);
    write_bytes(&mlt, 0, &fill(0x00));
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&head, 0, PAGE), head_written);
    assert_eq!(read_bytes(&mlt, 0, PAGE), mlt_image);
    assert!(!mark.exists());
}

/// A mark of a kind this binary does not know stays, and the clear is
/// `Corrupt`.
#[test]
fn clear_keeps_a_mark_of_an_unknown_kind() {
    let dir = TempDir::labeled("page-guard-unknown-kind").unwrap();
    let head = dir.path().join("header.head");
    let mark = dir.path().join("guard/header.head");
    arm_page_guard_group(&mark, &[("header.head", 0, fill(0x41).as_slice())]).unwrap();
    let mut raw = std::fs::read(&mark).unwrap();
    raw[6..8].copy_from_slice(&7u16.to_le_bytes());
    std::fs::write(&mark, &raw).unwrap();
    write_bytes(&head, 0, &fill(0x41));
    let head_file = open_rw(&head);
    let err = clear_page_guard(&mark, &head_file, &head).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
    assert_eq!(std::fs::read(&mark).unwrap(), raw);

    // The single-file kind 0 is no longer read either.
    raw[6..8].copy_from_slice(&0u16.to_le_bytes());
    std::fs::write(&mark, &raw).unwrap();
    let err = clear_page_guard(&mark, &head_file, &head).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
    assert_eq!(std::fs::read(&mark).unwrap(), raw);
}

#[test]
fn clear_of_a_missing_mark_syncs_its_directory() {
    let dir = TempDir::labeled("page-guard-missing-clear").unwrap();
    let live = dir.path().join("header.head");
    let mark = dir.path().join("guard").join("header.head");
    std::fs::create_dir_all(mark.parent().unwrap()).unwrap();
    write_bytes(&live, 0, &fill(0x11));
    let file = open_rw(&live);
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        clear_page_guard(&mark, &file, &live).unwrap();
        let synced = rbitcoin_store::file::take_fsynced_dirs();
        let parent = mark.parent().unwrap();
        assert!(
            synced.iter().any(|p| p == parent),
            "a missing mark must still sync its directory, got {synced:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    clear_page_guard(&mark, &file, &live).unwrap();
}

#[test]
fn group_restore_syncs_the_live_directory_before_the_mark_is_gone() {
    let dir = TempDir::labeled("page-guard-restore-dir").unwrap();
    let mark = dir.path().join("guard").join("class_c");
    let image = fill(0x5A);
    arm_page_guard_group(&mark, &[("fresh.body", 0, image.as_slice())]).unwrap();
    let live = dir.path().join("fresh.body");
    assert!(!live.exists());
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
        let synced = rbitcoin_store::file::take_fsynced_dirs();
        assert!(!mark.exists());
        let dir_at = synced.iter().position(|p| p == dir.path());
        let guard_at = synced.iter().position(|p| p == mark.parent().unwrap());
        assert!(
            dir_at.is_some() && guard_at.is_some() && dir_at < guard_at,
            "live dir must be durable before the mark is unlinked, got {synced:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&live, 0, PAGE), image);
}

#[test]
fn group_mark_count_that_cannot_fit_is_corrupt() {
    let dir = TempDir::labeled("page-guard-count").unwrap();
    let mark = dir.path().join("guard").join("class_c");
    std::fs::create_dir_all(mark.parent().unwrap()).unwrap();
    let mut raw = Vec::new();
    raw.extend_from_slice(&STORE_MAGIC);
    raw.extend_from_slice(&SCHEMA_VERSION.to_le_bytes());
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&3u32.to_le_bytes());
    assert_eq!(raw.len(), 12);
    std::fs::write(&mark, &raw).unwrap();
    let err = restore_page_guard_group(&mark, dir.path()).unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)), "{err:?}");
    assert!(mark.exists());
}

#[test]
fn first_arm_syncs_the_store_directory() {
    let dir = TempDir::labeled("page-guard-mkdir").unwrap();
    let mark = dir.path().join("guard").join("mark");
    assert!(!mark.parent().unwrap().exists());
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        arm_page_guard_group(&mark, &[("live", 0, &fill(0x01))]).unwrap();
        let synced = rbitcoin_store::file::take_fsynced_dirs();
        assert!(
            synced.iter().any(|p| p == dir.path()),
            "creating guard/ must sync the store directory, got {synced:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    arm_page_guard_group(&mark, &[("live", 0, &fill(0x01))]).unwrap();
    assert!(mark.is_file());
}

/// The release check uses the committed end, not the RAM high-water, and the
/// pages this arm names. A later page inside that high-water is still an
/// append when it shares no committed byte. A committed page the arm does
/// not name is rejected.
#[test]
fn write_outside_the_armed_pages_is_rejected() {
    let dir = TempDir::labeled("page-guard-range").unwrap();
    let path = dir.path().join("body");
    let file =
        rbitcoin_store::file::TableFile::create(&path, rbitcoin_primitives::TableKind::ScriptHash)
            .unwrap();
    let payload = vec![0x11u8; PAGE * 2];
    file.write_at(rbitcoin_store::file::FILE_HEADER_LEN as u64, &payload)
        .unwrap();
    let committed = file.logical_len();
    assert!(committed > PAGE as u64);
    assert!(!file.publish_end(committed + PAGE as u64 * 2, false));
    let image = vec![0u8; PAGE];
    arm_page_guard_group(
        &dir.path().join("guard/mark"),
        &[("body", 0, image.as_slice())],
    )
    .unwrap();
    let tail = (committed - 1) & !(PAGE as u64 - 1);
    let past = tail + PAGE as u64;
    file.write_at(past, &[0x22; 16]).unwrap();
    let err = file
        .write_at(PAGE as u64 + 256, &[0x33; 512])
        .expect_err("page 1 is committed and not armed");
    match err {
        StoreError::Rejected(msg) => assert_eq!(msg, "write is outside the armed pages"),
        other => panic!("expected reject, got {other:?}"),
    }
    file.write_at(64, &[0x44; 8]).unwrap();
}

fn committed_body(dir: &TempDir) -> (rbitcoin_store::file::TableFile, std::path::PathBuf) {
    let path = dir.path().join("body");
    let file =
        rbitcoin_store::file::TableFile::create(&path, rbitcoin_primitives::TableKind::ScriptHash)
            .unwrap();
    let payload = vec![0x11u8; PAGE * 2];
    file.write_at(rbitcoin_store::file::FILE_HEADER_LEN as u64, &payload)
        .unwrap();
    (file, path)
}

fn arm_page0(dir: &TempDir, mark: &str) {
    let image = vec![0u8; PAGE];
    arm_page_guard_group(&dir.path().join(mark), &[("body", 0, image.as_slice())]).unwrap();
}

fn page1_is_rejected(file: &rbitcoin_store::file::TableFile) {
    // Crosses a 512-byte sector boundary, so a tear can mix two sectors.
    let err = file
        .write_at(PAGE as u64 + 256, &[0x33; 512])
        .expect_err("page 1 is committed and still armed");
    match err {
        StoreError::Rejected(msg) => assert_eq!(msg, "write is outside the armed pages"),
        other => panic!("expected reject, got {other:?}"),
    }
}

/// A clear that unlinks the mark and then fails its directory sync blocks
/// the file: the unlink is not durable, and a crash could bring the mark
/// back over a later write. A retry that syncs releases it.
#[cfg(debug_assertions)]
#[test]
fn a_removed_mark_blocks_its_file_until_the_dir_sync_succeeds() {
    let dir = TempDir::labeled("page-guard-forget").unwrap();
    let live = dir.path().join("live");
    write_bytes(&live, 0, &fill(0x11));
    write_bytes(&live, PAGE as u64, &fill(0x12));
    let mark = dir.path().join("guard/live");
    arm_page_guard_group(&mark, &[("live", 0, fill(0x11).as_slice())]).unwrap();
    let committed = 2 * PAGE as u64;
    let check =
        |off: u64| rbitcoin_store::page_guard::check_armed_write(&live, committed, off, 512);
    check(256).expect("page 0 is armed");
    assert!(
        check(PAGE as u64 + 256).is_err(),
        "page 1 is committed and not armed"
    );
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    let file = open_rw(&live);
    assert!(clear_page_guard(&mark, &file, &live).is_err());
    assert!(!mark.exists());
    assert!(
        check(256).is_err(),
        "page 0 is open before the unlink is durable"
    );
    assert!(check(PAGE as u64 + 256).is_err());
    check(committed).expect("an append stays allowed");
    clear_page_guard(&mark, &file, &live).unwrap();
    check(256).expect("a synced retry releases the file");
    check(PAGE as u64 + 256).expect("a synced retry releases the file");
}

/// A clear that rewrites a two-file mark without one file, then fails the
/// directory sync, blocks the dropped file. The kept file stays armed.
#[cfg(debug_assertions)]
#[test]
fn a_dropped_file_stays_blocked_when_the_rewrite_sync_fails() {
    let dir = TempDir::labeled("page-guard-drop-block").unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    for live in [&a, &b] {
        write_bytes(live, 0, &fill(0x11));
        write_bytes(live, PAGE as u64, &fill(0x12));
    }
    let mark = dir.path().join("guard/pair");
    arm_page_guard_group(
        &mark,
        &[
            ("a", 0, fill(0x11).as_slice()),
            ("b", 0, fill(0x11).as_slice()),
        ],
    )
    .unwrap();
    let committed = 2 * PAGE as u64;
    let check =
        |live: &Path| rbitcoin_store::page_guard::check_armed_write(live, committed, 256, 512);
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    let file = open_rw(&a);
    assert!(clear_page_guard(&mark, &file, &a).is_err());
    assert!(mark.exists(), "b keeps the mark");
    assert!(
        check(&a).is_err(),
        "a is open before the rewrite is durable"
    );
    check(&b).expect("b is still armed");
    clear_page_guard(&mark, &file, &a).unwrap();
    check(&a).expect("a synced retry releases a");
}

/// A restore whose mark unlink fails its directory sync blocks the files it
/// wrote: the mark can come back and copy its images again.
#[cfg(debug_assertions)]
#[test]
fn a_restore_blocks_its_files_when_the_unlink_sync_fails() {
    let dir = TempDir::labeled("page-guard-restore-block").unwrap();
    let live = dir.path().join("live");
    write_bytes(&live, 0, &fill(0x11));
    let mark = dir.path().join("guard/live");
    arm_page_guard_group(&mark, &[("live", 0, fill(0x22).as_slice())]).unwrap();
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    assert!(restore_page_guard_group(&mark, dir.path()).is_err());
    assert!(
        rbitcoin_store::page_guard::check_armed_write(&live, PAGE as u64, 256, 512).is_err(),
        "the restored file is open before the unlink is durable"
    );
}

/// The arm rename makes the mark visible before its directory sync. A
/// failed sync must still leave the write check matching that mark.
#[cfg(debug_assertions)]
#[test]
fn an_arm_registers_its_pages_before_the_dir_sync() {
    let dir = TempDir::labeled("page-guard-arm-sync").unwrap();
    let live = dir.path().join("live");
    write_bytes(&live, 0, &fill(0x11));
    write_bytes(&live, PAGE as u64, &fill(0x12));
    let mark = dir.path().join("guard/live");
    rbitcoin_store::page_guard::fail_next_arm_dir_sync(&dir.path().join("guard"));
    assert!(arm_page_guard_group(&mark, &[("live", 0, fill(0x11).as_slice())]).is_err());
    assert!(mark.is_file(), "the rename published the mark");
    let committed = 2 * PAGE as u64;
    rbitcoin_store::page_guard::check_armed_write(&live, committed, 256, 512)
        .expect("page 0 is in the visible mark");
    assert!(
        rbitcoin_store::page_guard::check_armed_write(&live, committed, PAGE as u64 + 256, 512)
            .is_err(),
        "page 1 is committed and not in the mark"
    );
}

/// Without hole punching, `zero_range` writes zeros in 1 MiB chunks. Each
/// chunk publishes its end, and the next chunk must still be checked
/// against the committed end the call started with. A committed page the
/// arm does not name stays rejected.
#[cfg(debug_assertions)]
#[test]
fn zero_fill_fallback_keeps_one_committed_end() {
    let dir = TempDir::labeled("page-guard-zero-fill").unwrap();
    let (file, path) = committed_body(&dir);
    arm_page0(&dir, "guard/mark");
    let committed = file.logical_len();
    let past = ((committed - 1) & !(PAGE as u64 - 1)) + PAGE as u64;
    rbitcoin_store::file::skip_next_punch_hole(&path);
    file.zero_range(past + 1, 1024 * 1024 + 1)
        .expect("the whole range shares no committed page");
    match file.zero_range(PAGE as u64 + 256, 512) {
        Err(StoreError::Rejected(msg)) => assert_eq!(msg, "write is outside the armed pages"),
        other => panic!("expected reject, got {other:?}"),
    }
}

/// The sector rule for the release check: an append that starts at the
/// committed end inside its tail block is allowed on a file with a live arm.
/// An in-place write outside the armed pages is rejected, even inside one
/// sector: it may belong to the commit the mark protects.
#[test]
fn release_check_follows_the_sector_rule() {
    let dir = TempDir::labeled("page-guard-sector").unwrap();
    let (file, _path) = committed_body(&dir);
    let committed = file.logical_len();
    assert!(!committed.is_multiple_of(PAGE as u64));
    arm_page0(&dir, "guard/mark");
    file.write_at(committed, &[0x55; 64])
        .expect("an append inside the committed tail block");
    match file.write_at(PAGE as u64 + 8, &[0x66; 8]) {
        Err(StoreError::Rejected(msg)) => assert_eq!(msg, "write is outside the armed pages"),
        other => panic!("a one-sector write outside the arm went through: {other:?}"),
    }
    page1_is_rejected(&file);
}

/// A failed `commit_appends` may have published one length of its group
/// and not another. Every file in the group refuses writes until reopen.
#[cfg(debug_assertions)]
#[test]
fn a_failed_append_poisons_its_group_until_reopen() {
    use rbitcoin_primitives::TableKind;
    use rbitcoin_store::error::StoreError;
    use rbitcoin_store::file::{TableFile, FILE_HEADER_LEN};
    use rbitcoin_store::page_guard::{commit_appends, Append};
    let dir = rbitcoin_store::testutil::TempDir::labeled("append-poison").unwrap();
    let (pa, pb) = (dir.path.join("a"), dir.path.join("b"));
    let a = TableFile::create(&pa, TableKind::ArrayLink).unwrap();
    let b = TableFile::create(&pb, TableKind::ArrayLink).unwrap();
    let at = FILE_HEADER_LEN as u64;
    let row = [7u8; 8];
    let group = |a: &TableFile, b: &TableFile| {
        commit_appends(&[
            Append {
                file: a,
                at,
                bytes: &row,
                new_logical: at + 8,
            },
            Append {
                file: b,
                at,
                bytes: &row,
                new_logical: at + 8,
            },
        ])
    };
    // `a`'s length lands; `b`'s length flush fails.
    rbitcoin_store::io_handle::arm_fail_flush(b.io_handle(), 1);
    assert!(group(&a, &b).is_err());
    for f in [&a, &b] {
        let end = f.logical_len();
        for refused in [
            f.write_at_pwrite(FILE_HEADER_LEN as u64, &[1]),
            f.write_uncommitted(end, &[1]),
            f.set_logical_len(FILE_HEADER_LEN as u64),
            f.zero_range(FILE_HEADER_LEN as u64, 8),
            group(&a, &b),
        ] {
            match refused {
                Err(StoreError::Rejected(m)) => assert!(m.contains("reopen"), "{m}"),
                other => panic!("a write after a failed append: {other:?}"),
            }
        }
        // A publisher must not move the length either. A flush still
        // republishes the length RAM already holds.
        let header = |f: &TableFile| {
            let raw = std::fs::read(f.path()).unwrap();
            u64::from_le_bytes(raw[8..16].try_into().unwrap())
        };
        let before = header(f);
        for durable in [true, false] {
            assert!(
                !f.publish_end(end + 8, durable),
                "publish_end on a poisoned file"
            );
            assert!(
                !f.publish_locator_end(end + 8, durable),
                "publish_locator_end on a poisoned file"
            );
        }
        assert_eq!(f.logical_len(), end, "RAM length moved on a poisoned file");
        assert_eq!(header(f), before, "header moved on a poisoned file");
        f.flush().expect("a flush republishes the RAM length");
        assert_eq!(header(f), end);
    }
    drop((a, b));
    let a = TableFile::open(&pa, TableKind::ArrayLink).unwrap();
    a.write_at_pwrite(FILE_HEADER_LEN as u64, &[1])
        .expect("reopen clears the poison");
}
