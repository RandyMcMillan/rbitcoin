//! The write probe's own classes: a write counts once it lands, and a
//! length on a live mark that misses it is not safe.
use rbitcoin_store::file::TRAILING_FOOTER_LEN;
use rbitcoin_store::page_guard::{arm_page_guard_group, clear_page_guard, PAGE};
use rbitcoin_store::testutil::TempDir;
use rbitcoin_store::write_probe::{
    classify, opened_trailing, sync_begin, synced, Landing, WriteKind,
};
use std::path::{Path, PathBuf};

/// A store directory: `meta` beside the table files, so the probe keeps
/// their computed class. The table files themselves are not created: the
/// caller hands the probe the header end it would read.
struct Store(TempDir);

impl Store {
    fn new() -> Self {
        let dir = TempDir::labeled("write-probe").unwrap();
        std::fs::write(dir.path().join("meta"), b"").unwrap();
        Self(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.path().join(name)
    }

    /// Arm a real mark under `guard/` over `pages` of `name`.
    fn arm(&self, name: &str, pages: &[u64]) -> PathBuf {
        let mark = self.path("guard/probe");
        let image = vec![0u8; PAGE as usize];
        let group: Vec<(&str, u64, &[u8])> =
            pages.iter().map(|&p| (name, p, image.as_slice())).collect();
        arm_page_guard_group(&mark, &group).unwrap();
        mark
    }
}

fn payload(path: &Path, committed: u64, offset: u64, len: usize) -> Landing {
    classify(
        path,
        committed,
        offset,
        &vec![0; len],
        false,
        16,
        WriteKind::Payload,
    )
    .2
}

fn publish(path: &Path, committed: u64, new: u64) -> Option<&'static str> {
    classify(
        path,
        committed,
        8,
        &new.to_le_bytes(),
        false,
        16,
        WriteKind::Length,
    )
    .0
}

fn full_sync(path: &Path) {
    synced(path, sync_begin(path, None));
}

#[test]
fn a_sync_begun_before_a_payload_lands_does_not_clear_it() {
    let store = Store::new();
    let path = store.path("in-flight");
    let landing = payload(&path, 1000, 1000, 100);
    let early = sync_begin(&path, None);
    drop(landing);
    synced(&path, early);
    assert_eq!(publish(&path, 1000, 1100), Some("LEN_BEFORE_SYNC"));
    full_sync(&path);
    assert_eq!(publish(&path, 1000, 1100), Some("LEN_AFTER_SYNC"));
}

#[test]
fn a_sync_begun_before_a_footer_lands_does_not_make_it_durable() {
    let store = Store::new();
    let path = store.path("footer-in-flight");
    opened_trailing(&path, 100);
    let footer = TRAILING_FOOTER_LEN as u64;
    let (_, _, landing) = classify(
        &path,
        100,
        200 - footer,
        &200u64.to_le_bytes(),
        true,
        0,
        WriteKind::Length,
    );
    let early = sync_begin(&path, None);
    landing.landed();
    synced(&path, early);
    let (class, durable, _landing) = classify(&path, 0, 150, &[0; 10], true, 0, WriteKind::Payload);
    assert_eq!((class, durable), (None, 100));
}

fn footer(path: &Path, durable: u64, new: u64) -> Landing {
    let footer = TRAILING_FOOTER_LEN as u64;
    classify(
        path,
        durable,
        new - footer,
        // The whole footer, as `write_trailer` writes it: it ends at `new`.
        &[0; TRAILING_FOOTER_LEN],
        true,
        0,
        WriteKind::Length,
    )
    .2
}

#[test]
fn a_footer_write_that_failed_keeps_the_durable_end() {
    let store = Store::new();
    let path = store.path("footer-failed");
    opened_trailing(&path, 8192);
    // Dropped without `landed`: the write returned `Err`.
    drop(footer(&path, 8192, 4096));
    full_sync(&path);
    let (class, durable, _landing) =
        classify(&path, 0, 5000, &[0; 10], true, 0, WriteKind::Payload);
    assert_eq!((class, durable), (Some("ONE_SECTOR"), 8192));

    footer(&path, 8192, 4096).landed();
    full_sync(&path);
    let (class, durable, _landing) =
        classify(&path, 0, 5000, &[0; 10], true, 0, WriteKind::Payload);
    assert_eq!((class, durable), (None, 4096));
}

#[test]
fn a_synced_length_on_a_mark_that_misses_the_publish_is_not_safe() {
    let page = PAGE;
    let store = Store::new();
    let path = store.path("on-mark");
    store.arm("on-mark", &[2 * page]);
    drop(payload(&path, 2 * page, 2 * page, 100));
    full_sync(&path);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_ON_MARK")
    );
    // The same mark imaging the length page too restores both together.
    let mark = store.arm("on-mark", &[0]);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_AFTER_SYNC")
    );
    // A cleared mark images nothing, and the bytes are synced.
    store.arm("on-mark", &[2 * page]);
    let data = std::fs::File::create(&path).unwrap();
    clear_page_guard(&mark, &data, &path).unwrap();
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_AFTER_SYNC")
    );
}

#[test]
fn a_synced_length_with_no_mark_on_its_bytes_stays_safe() {
    let page = PAGE;
    let store = Store::new();
    let path = store.path("off-mark");
    store.arm("off-mark", &[page]);
    drop(payload(&path, 2 * page, 2 * page, 100));
    full_sync(&path);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_AFTER_SYNC")
    );
}

#[test]
fn a_stale_sync_does_not_replace_a_newer_durable_footer() {
    let store = Store::new();
    let path = store.path("footer-stale-sync");
    opened_trailing(&path, 100);
    let stale = sync_begin(&path, None);
    footer(&path, 100, 200).landed();
    full_sync(&path);
    synced(&path, stale);
    let (class, durable, _landing) = classify(&path, 0, 150, &[0; 10], true, 0, WriteKind::Payload);
    assert_eq!((class, durable), (Some("ONE_SECTOR"), 200));
}

fn footer_class(path: &Path, durable: u64, new: u64) -> Option<&'static str> {
    let footer = TRAILING_FOOTER_LEN as u64;
    classify(
        path,
        durable,
        new - footer,
        // The whole footer, as `write_trailer` writes it: it ends at `new`.
        &[0; TRAILING_FOOTER_LEN],
        true,
        0,
        WriteKind::Length,
    )
    .0
}

#[test]
fn a_growing_footer_across_a_sector_is_not_safe_after_its_payload_syncs() {
    let store = Store::new();
    let path = store.path("footer-two-sectors");
    opened_trailing(&path, 32);
    drop(classify(
        &path,
        0,
        0,
        &[0; 496],
        true,
        0,
        WriteKind::Payload,
    ));
    full_sync(&path);
    assert_eq!(footer_class(&path, 32, 528), Some("LEN_MULTI_SECTOR"));
    // A mark that images the footer's page restores it whole.
    store.arm("footer-two-sectors", &[0]);
    assert_eq!(footer_class(&path, 32, 528), Some("LEN_AFTER_SYNC"));
}

#[test]
fn a_growing_footer_in_one_sector_is_safe_after_its_payload_syncs() {
    let store = Store::new();
    let path = store.path("footer-one-sector");
    opened_trailing(&path, 32);
    drop(classify(
        &path,
        0,
        0,
        &[0; 400],
        true,
        0,
        WriteKind::Payload,
    ));
    full_sync(&path);
    assert_eq!(footer_class(&path, 32, 432), Some("LEN_AFTER_SYNC"));
}

#[test]
fn a_write_over_an_unsynced_shrink_is_below_the_durable_end() {
    let store = Store::new();
    let path = store.path("shrink");
    synced(&path, sync_begin(&path, Some(8192)));
    // `set_logical_len(4096)` wrote the shorter header; no sync yet.
    let (class, durable, _landing) =
        classify(&path, 4096, 4096, &[0; 1024], false, 16, WriteKind::Payload);
    assert_eq!((class, durable), (Some("MULTI_SECTOR"), 8192));
    synced(&path, sync_begin(&path, Some(4096)));
    let (class, durable, _landing) =
        classify(&path, 4096, 4096, &[0; 1024], false, 16, WriteKind::Payload);
    assert_eq!((class, durable), (None, 4096));
}

#[test]
fn a_cold_index_table_keeps_its_class() {
    // A split store: the cold directory has no `meta`, and the index keeps
    // its own under `blockfilter.idx/`, beside the body's directory.
    let cold = TempDir::labeled("write-probe-cold").unwrap();
    let body = cold.path().join("blockfilter.body");
    std::fs::create_dir_all(&body).unwrap();
    let path = body.join("000001");
    let (class, _, _landing) =
        classify(&path, 8192, 600, &[0; 1024], false, 16, WriteKind::Payload);
    assert_eq!(class, Some("MULTI_SECTOR"));
    // A table a test builds outside any store stays a fixture.
    let bare = TempDir::labeled("write-probe-bare").unwrap();
    let path = bare.path().join("table");
    let (class, _, _landing) =
        classify(&path, 8192, 600, &[0; 1024], false, 16, WriteKind::Payload);
    assert_eq!(class, Some("FIXTURE"));
}
