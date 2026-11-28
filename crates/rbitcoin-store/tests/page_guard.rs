//! A mark of whole 4 KiB images, and an append that stays off those pages.
use rbitcoin_primitives::{SCHEMA_VERSION, STORE_MAGIC};
use rbitcoin_store::page_guard::{
    append_past_end, arm_page_guard_group, clear_page_guard, clear_page_guard_rel, clear_sh_marks,
    guard_rel_name, restore_page_guard_group, restore_page_guard_group_fit,
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

/// A clear that finds no mark is done: it syncs nothing, because a sync
/// after a failed one is not trusted to make an earlier unlink durable.
#[test]
fn clear_of_a_missing_mark_syncs_nothing() {
    let dir = TempDir::labeled("page-guard-clear-missing").unwrap();
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
        assert!(synced.is_empty(), "a missing mark synced {synced:?}");
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

/// An arm syncs the store directory that holds `meta` before it publishes
/// the mark, so a `meta` rename whose directory sync failed is durable
/// before any mark.
#[test]
fn arm_on_meta_31_syncs_the_directory_that_holds_meta() {
    let dir = TempDir::labeled("page-guard-meta-retry").unwrap();
    let mut meta = STORE_MAGIC.to_vec();
    meta.extend_from_slice(&31u16.to_le_bytes());
    std::fs::write(dir.path().join("meta"), &meta).unwrap();
    std::fs::create_dir(dir.path().join("guard")).unwrap();
    let page = fill(0x01);
    #[cfg(all(unix, debug_assertions))]
    {
        rbitcoin_store::file::arm_fsync_dir_trace();
        rbitcoin_store::page_guard::arm_store_group(
            dir.path(),
            rbitcoin_store::page_guard::CLASS_C_MARK,
            &[("confirmed.body", 0, page.as_slice())],
        )
        .unwrap();
        let dirs = rbitcoin_store::file::take_fsynced_dirs();
        let store_at = dirs.iter().position(|d| d == dir.path());
        let guard_at = dirs.iter().position(|d| *d == dir.path().join("guard"));
        assert!(
            matches!((store_at, guard_at), (Some(s), Some(g)) if s < g),
            "meta 31 must be durable before the mark, got {dirs:?}"
        );
    }
    #[cfg(not(all(unix, debug_assertions)))]
    rbitcoin_store::page_guard::arm_store_group(
        dir.path(),
        rbitcoin_store::page_guard::CLASS_C_MARK,
        &[("confirmed.body", 0, page.as_slice())],
    )
    .unwrap();
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

#[test]
fn group_restore_accepts_a_two_component_name() {
    let dir = TempDir::labeled("page-guard-rel").unwrap();
    let live = dir.path().join("scripthash.body").join("00");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    let mark = dir.path().join("guard/sh-00");
    write_bytes(&live, 0, &fill(0x11));
    let next = fill(0xA1);
    arm_page_guard_group(&mark, &[("scripthash.body/00", 0, next.as_slice())]).unwrap();
    write_bytes(&live, 0, &fill(0x00));
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    assert_eq!(read_bytes(&live, 0, PAGE), next);
}

#[test]
fn fit_restore_does_not_extend_a_short_file() {
    let dir = TempDir::labeled("page-guard-fit").unwrap();
    let live = dir.path().join("scripthash.head.val");
    let mark = dir.path().join("guard/sh-00");
    let mut image = fill(0x22);
    image[8] = 0x33;
    write_bytes(&live, 0, &image[..16]);
    arm_page_guard_group(&mark, &[("scripthash.head.val", 0, image.as_slice())]).unwrap();
    write_bytes(&live, 0, &[0xffu8; 16]);
    assert_eq!(std::fs::metadata(&live).unwrap().len(), 16);
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    assert_eq!(std::fs::metadata(&live).unwrap().len(), 16);
    assert_eq!(read_bytes(&live, 0, 16), image[..16]);
}

#[test]
fn guard_rel_name_uses_forward_slashes() {
    let dir = TempDir::labeled("page-guard-slash").unwrap();
    let live = dir.path().join("scripthash.ovf").join("ingest");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    std::fs::File::create(&live).unwrap();
    assert_eq!(
        guard_rel_name(dir.path(), &live).unwrap(),
        "scripthash.ovf/ingest"
    );
}

#[test]
fn fit_restore_drops_the_ahead_sidecar() {
    let dir = TempDir::labeled("page-guard-ahead").unwrap();
    let live = dir.path().join("scripthash.ovf").join("ingest");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    let ahead = live.parent().unwrap().join("ingest.ahead");
    let mark = dir.path().join("guard/sh-ovf");
    let image = fill(0x44);
    write_bytes(&live, 0, &image);
    write_bytes(&ahead, 0, b"SHA1ahead");
    arm_page_guard_group(&mark, &[("scripthash.ovf/ingest", 0, image.as_slice())]).unwrap();
    write_bytes(&live, 0, &fill(0x00));
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    assert!(
        !ahead.exists(),
        "ahead replay would overwrite the restored page"
    );
    assert_eq!(read_bytes(&live, 0, PAGE), image);
}

/// A crash can leave a truncated `guard/sh-*.tmp`. Dropping a name skips it,
/// as restore does, and leaves it for reset to remove.
#[test]
fn drop_sh_rel_skips_an_unpublished_temp() {
    let dir = TempDir::labeled("page-guard-drop-tmp").unwrap();
    std::fs::create_dir_all(dir.path().join("guard")).unwrap();
    let tmp = dir.path().join("guard/sh-ovf.tmp");
    std::fs::write(&tmp, b"half a mark").unwrap();
    rbitcoin_store::page_guard::drop_sh_rel(dir.path(), "scripthash.ovf/ingest").unwrap();
    assert!(tmp.is_file());
}

/// A restore whose `.ahead` sidecar is already gone still syncs the
/// directory that held it. An earlier unlink may have landed without that
/// sync, and the mark goes away next.
#[test]
fn fit_restore_syncs_the_sidecar_directory_when_it_is_gone() {
    let dir = TempDir::labeled("page-guard-ahead-gone").unwrap();
    let live = dir.path().join("scripthash.ovf").join("ingest");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    let mark = dir.path().join("guard/sh-ovf");
    let image = fill(0x45);
    write_bytes(&live, 0, &image);
    arm_page_guard_group(&mark, &[("scripthash.ovf/ingest", 0, image.as_slice())]).unwrap();
    #[cfg(all(unix, debug_assertions))]
    rbitcoin_store::file::arm_fsync_dir_trace();
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    #[cfg(all(unix, debug_assertions))]
    {
        let dirs = rbitcoin_store::file::take_fsynced_dirs();
        let side_dir = live.parent().unwrap();
        assert!(
            dirs.iter().any(|d| d == side_dir),
            "the sidecar directory was not synced, got {dirs:?}"
        );
    }
}

/// A restore that creates a file one directory down syncs that directory
/// before the mark goes away, so a second crash cannot lose the name.
#[cfg(all(unix, debug_assertions))]
#[test]
fn group_restore_syncs_a_nested_file_parent_before_the_mark_is_gone() {
    let dir = TempDir::labeled("page-guard-restore-nested").unwrap();
    let sub = dir.path().join("scripthash.body");
    std::fs::create_dir_all(&sub).unwrap();
    let mark = dir.path().join("guard").join("sh-00");
    let image = fill(0x5B);
    arm_page_guard_group(&mark, &[("scripthash.body/00", 0, image.as_slice())]).unwrap();
    let live = sub.join("00");
    assert!(!live.exists());
    rbitcoin_store::file::arm_fsync_dir_trace();
    assert!(restore_page_guard_group(&mark, dir.path()).unwrap());
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    let sub_at = synced.iter().position(|p| p == &sub);
    let guard_at = synced.iter().position(|p| p == mark.parent().unwrap());
    assert!(
        sub_at.is_some() && guard_at.is_some() && sub_at < guard_at,
        "the nested live dir must be durable before the mark is unlinked, got {synced:?}"
    );
    assert_eq!(read_bytes(&live, 0, PAGE), image);
}

/// An empty live file takes every image in full, whatever the record order.
#[test]
fn fit_restore_fills_an_empty_file_with_every_page() {
    let dir = TempDir::labeled("page-guard-fit-empty").unwrap();
    let live = dir.path().join("scripthash.ovf").join("ingest");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    std::fs::File::create(&live).unwrap();
    let mark = dir.path().join("guard/sh-ovf");
    let (low, high) = (fill(0x61), fill(0x62));
    arm_page_guard_group(
        &mark,
        &[
            ("scripthash.ovf/ingest", 0, low.as_slice()),
            ("scripthash.ovf/ingest", PAGE as u64, high.as_slice()),
        ],
    )
    .unwrap();
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    assert_eq!(std::fs::metadata(&live).unwrap().len(), 2 * PAGE as u64);
    assert_eq!(read_bytes(&live, 0, PAGE), low);
    assert_eq!(read_bytes(&live, PAGE as u64, PAGE), high);
}

/// A fit restore of an empty file that a crash stops after any of its
/// writes keeps the mark, and the next restore still ends with every page.
#[cfg(debug_assertions)]
#[test]
fn an_interrupted_fit_restore_of_an_empty_file_finishes_on_retry() {
    use rbitcoin_store::file::{arm_write_trace, take_write_trace, TracedWrite};
    let dir = TempDir::labeled("page-guard-fit-empty-retry").unwrap();
    let live = dir.path().join("scripthash.ovf").join("ingest");
    std::fs::create_dir_all(live.parent().unwrap()).unwrap();
    std::fs::File::create(&live).unwrap();
    let mark = dir.path().join("guard/sh-ovf");
    let (low, high) = (fill(0x61), fill(0x62));
    arm_page_guard_group(
        &mark,
        &[
            ("scripthash.ovf/ingest", 0, low.as_slice()),
            ("scripthash.ovf/ingest", PAGE as u64, high.as_slice()),
        ],
    )
    .unwrap();
    let armed = std::fs::read(&mark).unwrap();
    arm_write_trace();
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    let ops: Vec<TracedWrite> = take_write_trace()
        .into_iter()
        .filter(|w| match w {
            TracedWrite::Write { path, .. } | TracedWrite::SetLen { path, .. } => path == &live,
        })
        .collect();
    // The prefixes below cover the first-page-only state only when the
    // restore writes page 0 and then page 1 as two whole-page writes.
    let page_write = |want: u64| {
        ops.iter().position(|w| {
            matches!(w, TracedWrite::Write { offset, len, .. }
                if *offset == want && *len == PAGE as u64)
        })
    };
    match (page_write(0), page_write(PAGE as u64)) {
        (Some(low_at), Some(high_at)) if low_at < high_at => {}
        _ => panic!("the restore must write page 0, then page 1: {ops:?}"),
    }
    for stop in 1..ops.len() {
        let file = OpenOptions::new().write(true).open(&live).unwrap();
        file.set_len(0).unwrap();
        drop(file);
        for op in &ops[..stop] {
            match op {
                TracedWrite::Write { offset, len, .. } => {
                    let image = if *offset < PAGE as u64 { &low } else { &high };
                    write_bytes(&live, *offset, &image[..*len as usize]);
                }
                TracedWrite::SetLen { len, .. } => {
                    let file = OpenOptions::new().write(true).open(&live).unwrap();
                    file.set_len(*len).unwrap();
                }
            }
        }
        std::fs::write(&mark, &armed).unwrap();
        assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
        assert_eq!(
            std::fs::metadata(&live).unwrap().len(),
            2 * PAGE as u64,
            "stopped after {stop} of {ops:?}"
        );
        assert_eq!(read_bytes(&live, 0, PAGE), low);
        assert_eq!(read_bytes(&live, PAGE as u64, PAGE), high);
    }
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

/// `clear_page_guard_rel` drops the mark. The release check must drop the
/// in-memory pages with it, or the next committed write is rejected.
#[test]
fn clear_rel_forgets_the_armed_pages() {
    let dir = TempDir::labeled("page-guard-clear-rel").unwrap();
    let (file, path) = committed_body(&dir);
    let mark = dir.path().join("guard/mark");
    arm_page0(&dir, "guard/mark");
    page1_is_rejected(&file);
    clear_page_guard_rel(&mark, dir.path(), &[(&file.read_file, path.as_path())]).unwrap();
    assert!(!mark.exists(), "clear removes the mark");
    file.write_at(PAGE as u64, &[0x55; 16]).unwrap();
}

/// A fit restore deletes the mark outright. The armed-page record goes with it.
#[test]
fn fit_restore_forgets_the_armed_pages() {
    let dir = TempDir::labeled("page-guard-fit-forget").unwrap();
    let (file, _path) = committed_body(&dir);
    let mark = dir.path().join("guard/mark");
    arm_page0(&dir, "guard/mark");
    page1_is_rejected(&file);
    assert!(restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    assert!(!mark.exists(), "restore removes the mark");
    file.write_at(PAGE as u64, &[0x66; 16]).unwrap();
}

/// `clear_sh_marks` deletes `guard/sh-*` without a retain rewrite.
#[test]
fn clear_sh_marks_forgets_the_armed_pages() {
    let dir = TempDir::labeled("page-guard-sh-forget").unwrap();
    let (file, _path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-ovf");
    page1_is_rejected(&file);
    clear_sh_marks(dir.path()).unwrap();
    assert!(!dir.path().join("guard/sh-ovf").exists());
    file.write_at(PAGE as u64, &[0x77; 16]).unwrap();
}

/// `clear_sh_marks` can unlink a mark and then fail its directory sync. The
/// unlink may not be durable, and a retried sync is not trusted, so the file
/// stays latched after a retry that finds no mark.
#[cfg(all(unix, debug_assertions))]
#[test]
fn clear_sh_marks_retry_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-sh-retry").unwrap();
    let (file, _path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-ovf");
    let guard = dir.path().join("guard");
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&guard);
    assert!(clear_sh_marks(dir.path()).is_err());
    assert!(!guard.join("sh-ovf").exists(), "the unlink landed");
    assert!(
        file.write_at(PAGE as u64 + 256, &[0x44; 16]).is_err(),
        "the file took a write before the unlink was durable"
    );
    clear_sh_marks(dir.path()).unwrap();
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// `restore_sh_marks` copies the last `sh-*` mark, unlinks it, and then
/// fails the `guard/` sync, which latches the file. A retry in the same
/// process finds no mark and leaves the file latched until restart.
#[cfg(all(unix, debug_assertions))]
#[test]
fn restore_sh_marks_retry_with_no_mark_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-sh-restore-retry").unwrap();
    let (file, _path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-ovf");
    let guard = dir.path().join("guard");
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&guard);
    assert!(rbitcoin_store::page_guard::restore_sh_marks(dir.path()).is_err());
    assert!(!guard.join("sh-ovf").exists(), "the unlink landed");
    assert!(
        file.write_at(PAGE as u64 + 256, &[0x44; 16]).is_err(),
        "the file took a write before the unlink was durable"
    );
    rbitcoin_store::page_guard::restore_sh_marks(dir.path()).unwrap();
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// `restore_page_guard_group_fit` unlinks its mark and then fails the
/// `guard/` sync. A retry on the same mark finds it gone, returns
/// `Ok(false)`, and leaves the file latched.
#[cfg(all(unix, debug_assertions))]
#[test]
fn restore_fit_retry_with_no_mark_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-fit-restore-retry").unwrap();
    let (file, _path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-ovf");
    let guard = dir.path().join("guard");
    let mark = guard.join("sh-ovf");
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&guard);
    assert!(restore_page_guard_group_fit(&mark, dir.path()).is_err());
    assert!(!mark.exists(), "the unlink landed");
    assert!(
        file.write_at(PAGE as u64 + 256, &[0x44; 16]).is_err(),
        "the file took a write before the unlink was durable"
    );
    assert!(!restore_page_guard_group_fit(&mark, dir.path()).unwrap());
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// `drop_sh_rel` drops the only name in the last `sh-*` mark, so the mark is
/// unlinked, and then the `guard/` sync fails, which latches the file. A
/// retry finds no mark and leaves the file latched until restart.
#[cfg(all(unix, debug_assertions))]
#[test]
fn drop_sh_rel_retry_with_no_mark_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-drop-rel-retry").unwrap();
    let (file, _path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-ovf");
    let guard = dir.path().join("guard");
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&guard);
    assert!(rbitcoin_store::page_guard::drop_sh_rel(dir.path(), "body").is_err());
    assert!(!guard.join("sh-ovf").exists(), "the unlink landed");
    assert!(
        file.write_at(PAGE as u64 + 256, &[0x44; 16]).is_err(),
        "the file took a write before the unlink was durable"
    );
    rbitcoin_store::page_guard::drop_sh_rel(dir.path(), "body").unwrap();
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// `restore_page_guard_group` copies its mark, unlinks it, and then fails
/// the `guard/` sync, which latches the restored file. A retry on the same
/// mark finds it gone, returns `Ok(false)`, and leaves the file latched.
#[cfg(all(unix, debug_assertions))]
#[test]
fn restore_retry_with_no_mark_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-restore-retry").unwrap();
    let live = dir.path().join("live");
    write_bytes(&live, 0, &fill(0x11));
    let mark = dir.path().join("guard/live");
    arm_page_guard_group(&mark, &[("live", 0, fill(0x22).as_slice())]).unwrap();
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    assert!(restore_page_guard_group(&mark, dir.path()).is_err());
    assert!(!mark.exists(), "the unlink landed");
    assert!(
        rbitcoin_store::page_guard::check_armed_write(&live, PAGE as u64, 256, 512).is_err(),
        "the restored file is open before the unlink is durable"
    );
    assert!(!restore_page_guard_group(&mark, dir.path()).unwrap());
    assert!(
        rbitcoin_store::page_guard::check_armed_write(&live, PAGE as u64, 256, 512).is_err(),
        "a retry does not release a latched file"
    );
}

/// `clear_sh_marks` unlinks `sh-a`, then fails to unlink `sh-b`. The file
/// `sh-a` named latches, because that unlink is not durable yet, and the
/// retry that finds `sh-a` gone leaves it latched.
#[cfg(all(unix, debug_assertions))]
#[test]
fn clear_sh_marks_latches_an_earlier_unlink_when_a_later_one_fails() {
    let dir = TempDir::labeled("page-guard-sh-partial").unwrap();
    let (file, path) = committed_body(&dir);
    arm_page0(&dir, "guard/sh-a");
    let guard = dir.path().join("guard");
    // A directory where a mark should be: its unlink fails.
    std::fs::create_dir(guard.join("sh-b")).unwrap();
    assert!(clear_sh_marks(dir.path()).is_err());
    assert!(!guard.join("sh-a").exists(), "sh-a went before sh-b");
    let committed = rbitcoin_store::file::FILE_HEADER_LEN as u64 + 2 * PAGE as u64;
    assert!(
        rbitcoin_store::page_guard::check_armed_write(&path, committed, 256, 16).is_err(),
        "page 0 took a write before the sh-a unlink was durable"
    );
    std::fs::remove_dir(guard.join("sh-b")).unwrap();
    clear_sh_marks(dir.path()).unwrap();
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// `clear_page_guard_rel` unlinks the mark, then fails the `guard/` sync.
/// The retry finds no mark and leaves the file latched.
#[cfg(debug_assertions)]
#[test]
fn clear_rel_retry_with_no_mark_keeps_the_file_latched() {
    let dir = TempDir::labeled("page-guard-rel-retry").unwrap();
    let (file, path) = committed_body(&dir);
    let mark = dir.path().join("guard/mark");
    arm_page0(&dir, "guard/mark");
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    let files = [(&file.read_file, path.as_path())];
    assert!(clear_page_guard_rel(&mark, dir.path(), &files).is_err());
    assert!(!mark.exists(), "the unlink landed");
    assert!(
        file.write_at(PAGE as u64 + 256, &[0x44; 16]).is_err(),
        "the file took a write before the unlink was durable"
    );
    clear_page_guard_rel(&mark, dir.path(), &files).unwrap();
    assert!(
        file.write_at(PAGE as u64, &[0x77; 16]).is_err(),
        "a retry does not release a latched file"
    );
}

/// A clear that unlinks the mark and then fails its directory sync latches
/// the file: the unlink is not durable, and a crash could bring the mark
/// back over a later write. A retry does not release it, and neither does a
/// new arm.
#[cfg(debug_assertions)]
#[test]
fn a_removed_mark_latches_its_file_when_the_dir_sync_fails() {
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
    assert!(
        check(256).is_err(),
        "a retry does not release a latched file"
    );
    assert!(
        check(PAGE as u64 + 256).is_err(),
        "a retry does not release a latched file"
    );
    assert!(
        arm_page_guard_group(&mark, &[("live", 0, fill(0x11).as_slice())]).is_err(),
        "a latched file takes no new arm"
    );
}

/// A clear that rewrites a two-file mark without one file, then fails the
/// directory sync, latches the dropped file. The kept file stays armed.
#[cfg(debug_assertions)]
#[test]
fn a_dropped_file_latches_when_the_rewrite_sync_fails() {
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
    assert!(
        check(&a).is_err(),
        "a retry does not release a latched file"
    );
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

/// A clear that rewrites the mark without one file and then fails its
/// directory sync latches that file: the rename is not durable, and a crash
/// could bring the old mark back over a later write. A new arm of that file
/// is refused too.
#[cfg(debug_assertions)]
#[test]
fn a_rewritten_mark_blocks_dropped_pages_when_the_dir_sync_fails() {
    let dir = TempDir::labeled("page-guard-retain-forget").unwrap();
    let live = dir.path().join("live");
    write_bytes(&live, 0, &fill(0x11));
    write_bytes(&live, PAGE as u64, &fill(0x12));
    let mark = dir.path().join("guard/live");
    arm_page_guard_group(
        &mark,
        &[
            ("live", 0, fill(0x11).as_slice()),
            ("other", 0, fill(0x21).as_slice()),
        ],
    )
    .unwrap();
    let committed = 2 * PAGE as u64;
    let check = |off: u64| rbitcoin_store::page_guard::check_armed_write(&live, committed, off, 8);
    check(0).expect("page 0 is armed");
    assert!(
        check(PAGE as u64).is_err(),
        "page 1 is committed and not armed"
    );
    rbitcoin_store::page_guard::fail_next_mark_dir_sync(&dir.path().join("guard"));
    let names = std::collections::HashSet::from(["live".to_string()]);
    assert!(rbitcoin_store::page_guard::drop_page_guard_names(&mark, &names).is_err());
    assert!(mark.exists(), "the mark still names `other`");
    match check(0) {
        Err(StoreError::Rejected(msg)) => {
            assert_eq!(msg, "a guard mark change for this file is not durable")
        }
        other => panic!("`live` is open before the rewrite is durable: {other:?}"),
    }
    assert!(check(PAGE as u64).is_err());
    assert!(
        arm_page_guard_group(&mark, &[("live", PAGE as u64, fill(0x12).as_slice())]).is_err(),
        "a latched file takes no new arm"
    );
    assert!(
        check(PAGE as u64).is_err(),
        "a retry does not release a latched file"
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
