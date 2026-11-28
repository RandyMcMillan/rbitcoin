//! The write probe's own classes: a write counts once it lands, and a
//! length on a live mark that misses it is not safe.
use rbitcoin_store::file::TRAILING_FOOTER_LEN;
use rbitcoin_store::page_guard::{probe_arm_for_test, PAGE};
use rbitcoin_store::write_probe::{
    classify, opened_trailing, sync_begin, synced, Landing, WriteKind,
};
use std::path::{Path, PathBuf};

/// A path no other test, and no store, uses. Nothing is created there:
/// the probe falls back to the `committed` end it is given.
fn probe_path(name: &str) -> PathBuf {
    PathBuf::from(format!("/nonexistent/write-probe-test/{name}"))
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
    synced(path, sync_begin(path));
}

#[test]
fn a_sync_begun_before_a_payload_lands_does_not_clear_it() {
    let path = probe_path("in-flight");
    let landing = payload(&path, 1000, 1000, 100);
    let early = sync_begin(&path);
    drop(landing);
    synced(&path, early);
    assert_eq!(publish(&path, 1000, 1100), Some("LEN_BEFORE_SYNC"));
    full_sync(&path);
    assert_eq!(publish(&path, 1000, 1100), Some("LEN_AFTER_SYNC"));
}

#[test]
fn a_sync_begun_before_a_footer_lands_does_not_make_it_durable() {
    let path = probe_path("footer-in-flight");
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
    let early = sync_begin(&path);
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
    let path = probe_path("footer-failed");
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
    let path = probe_path("on-mark");
    probe_arm_for_test(&path, &[2 * page]);
    drop(payload(&path, 2 * page, 2 * page, 100));
    full_sync(&path);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_ON_MARK")
    );
    // The same mark imaging the length page too restores both together.
    probe_arm_for_test(&path, &[0, 2 * page]);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_AFTER_SYNC")
    );
}

#[test]
fn a_synced_length_with_no_mark_on_its_bytes_stays_safe() {
    let page = PAGE;
    let path = probe_path("off-mark");
    probe_arm_for_test(&path, &[page]);
    drop(payload(&path, 2 * page, 2 * page, 100));
    full_sync(&path);
    assert_eq!(
        publish(&path, 2 * page, 2 * page + 100),
        Some("LEN_AFTER_SYNC")
    );
}

#[test]
fn a_stale_sync_does_not_replace_a_newer_durable_footer() {
    let path = probe_path("footer-stale-sync");
    opened_trailing(&path, 100);
    let stale = sync_begin(&path);
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
    let path = probe_path("footer-two-sectors");
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
    probe_arm_for_test(&path, &[0]);
    assert_eq!(footer_class(&path, 32, 528), Some("LEN_AFTER_SYNC"));
}

#[test]
fn a_growing_footer_in_one_sector_is_safe_after_its_payload_syncs() {
    let path = probe_path("footer-one-sector");
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
