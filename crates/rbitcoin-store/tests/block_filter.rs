use rbitcoin_primitives::{Fk, Height};
use rbitcoin_store::block_filter::*;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::FILE_HEADER_LEN;
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

fn tmp_dir() -> rbitcoin_store::testutil::TempDir {
    rbitcoin_store::testutil::TempDir::labeled("blockfilter").unwrap()
}

fn slot(h: u32) -> BlockFilterSlot {
    BlockFilterSlot {
        header_fk: Fk(u64::from(h) + 100),
        filter_hash: [h as u8; 32],
        filter_header: [h as u8 ^ 0xff; 32],
    }
}

fn put_range(t: &BlockFilterTable, from: u32, to: u32) {
    let bodies: Vec<Vec<u8>> = (from..=to).map(|h| vec![h as u8; 3 + h as usize]).collect();
    let recs: Vec<BlockFilterRecord<'_>> = (from..=to)
        .zip(&bodies)
        .map(|(h, b)| BlockFilterRecord {
            height: Height(h),
            slot: slot(h),
            filter: b,
        })
        .collect();
    t.put(&recs).unwrap();
}

fn set_file_hwm(path: &Path, hwm: u64) {
    let mut f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    f.set_len(hwm.max(FILE_HEADER_LEN as u64)).unwrap();
    let mut hdr = [0u8; FILE_HEADER_LEN];
    f.seek(SeekFrom::Start(0)).unwrap();
    f.read_exact(&mut hdr).unwrap();
    hdr[8..16].copy_from_slice(&hwm.to_le_bytes());
    f.seek(SeekFrom::Start(0)).unwrap();
    f.write_all(&hdr).unwrap();
}

#[test]
fn filter_at_height_reads_only_its_record_and_survives_reopen() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(0));
    assert!(t.filter(Height(0)).unwrap().is_none());
    put_range(&t, 0, 9);
    assert_eq!(t.next_height(), Height(10));
    drop(t);

    // Scribble over every other record's body bytes: a lookup must not
    // walk earlier records to find its own.
    let body = BlockFilterTable::seg_dirs(&dir).body_path(0);
    let mut raw = fs::read(&body).unwrap();
    let start7 = FILE_HEADER_LEN + (0..7).map(|h| 3 + h).sum::<usize>();
    let len7 = 3 + 7;
    for (i, b) in raw.iter_mut().enumerate().skip(FILE_HEADER_LEN) {
        if !(start7..start7 + len7).contains(&i) {
            *b = 0xee;
        }
    }
    fs::write(&body, &raw).unwrap();

    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(10));
    let (bytes, s) = t.filter(Height(7)).unwrap().unwrap();
    assert_eq!(bytes, vec![7u8; 10]);
    assert_eq!(s, slot(7));
    assert_eq!(t.slot(Height(9)).unwrap(), Some(slot(9)));
    assert_eq!(
        t.slots(Height(3), Height(9)).unwrap().unwrap(),
        (3..=9).map(slot).collect::<Vec<_>>()
    );
    assert!(t.slots(Height(3), Height(10)).unwrap().is_none());
    let run = t.filters(Height(6), Height(8)).unwrap().unwrap();
    assert_eq!(
        run[1],
        (vec![7u8; 10], slot(7)),
        "range read matches single"
    );
    assert!(t.slot(Height(10)).unwrap().is_none());
    let gap = t.put(&[BlockFilterRecord {
        height: Height(11),
        slot: slot(11),
        filter: &[1],
    }]);
    assert!(matches!(gap, Err(StoreError::Corrupt(m)) if m.contains("next height")));
}

/// A same-length body for a slot that is no longer the row must not land.
#[test]
fn repair_body_leaves_a_replaced_slot_unchanged() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 1);
    let (before, live) = t.filter(Height(1)).unwrap().unwrap();
    let other = BlockFilterSlot {
        header_fk: Fk(999),
        ..live
    };
    assert_ne!(other, live);
    let wrote = t
        .repair_body(Height(1), &other, &vec![0xab; before.len()])
        .unwrap();
    assert!(!wrote, "repair_body wrote a body for a slot that is gone");
    let (after, slot_now) = t.filter(Height(1)).unwrap().unwrap();
    assert_eq!(after, before);
    assert_eq!(slot_now, live);
}

/// A repair rewrites a whole filter in place, across sectors at real sizes.
/// It writes only the live slot's length, and the reader gets the new bytes.
/// The write probe sees this one as the multi-sector repair it is.
#[test]
fn repair_body_rewrites_a_filter_across_sectors() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    let big = vec![0x31u8; 1_500];
    t.put(&[BlockFilterRecord {
        height: Height(0),
        slot: slot(0),
        filter: &big,
    }])
    .unwrap();
    let fixed = vec![0x32u8; big.len()];
    assert!(t.repair_body(Height(0), &slot(0), &fixed).unwrap());
    assert_eq!(t.filter(Height(0)).unwrap().unwrap(), (fixed, slot(0)));
}

/// Checkpoint headers are heights 1000, 2000, … and nothing in between.
#[test]
fn checkpoint_headers_are_every_thousandth_slot() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2_500);
    assert_eq!(
        t.checkpoint_headers(Height(999)).unwrap(),
        Some(Vec::<[u8; 32]>::new())
    );
    assert_eq!(
        t.checkpoint_headers(Height(2_500)).unwrap(),
        Some(vec![slot(1_000).filter_header, slot(2_000).filter_header])
    );
    assert!(t.checkpoint_headers(Height(2_501)).unwrap().is_none());
}

/// Crash states a live session cannot produce on demand: a commit that
/// synced body bytes but not its slots, then an idx HWM covering a slot
/// whose record is cut short. Open keeps the committed prefix each time.
#[test]
fn open_keeps_the_committed_prefix_after_torn_commits() {
    let dir = tmp_dir();
    let body = BlockFilterTable::seg_dirs(&dir).body_path(0);
    let end_of = |h: u64| FILE_HEADER_LEN as u64 + (0..=h).map(|i| 3 + i).sum::<u64>();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 4);
    drop(t);

    set_file_hwm(&body, end_of(4) + 50);
    {
        let mut f = fs::OpenOptions::new().write(true).open(&body).unwrap();
        f.seek(SeekFrom::Start(end_of(4))).unwrap();
        f.write_all(&[0xab; 50]).unwrap();
    }
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(5));
    assert_eq!(t.filter(Height(4)).unwrap().unwrap().0, vec![4u8; 7]);
    put_range(&t, 5, 5);
    assert_eq!(t.filter(Height(5)).unwrap().unwrap().0, vec![5u8; 8]);
    drop(t);

    set_file_hwm(&body, end_of(2) + 1);
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(3), "record 3 is torn");
    put_range(&t, 3, 4);
    assert_eq!(t.filter(Height(4)).unwrap().unwrap().0, vec![4u8; 7]);
}

#[test]
fn rolls_a_segment_when_the_next_start_passes_u32() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    drop(t);
    // Record 0 now ends one past u32::MAX (sparse body, slot len widened).
    let big = u32::MAX - FILE_HEADER_LEN as u32 + 1;
    let idx = BlockFilterTable::seg_dirs(&dir).idx_path(0);
    let mut raw = fs::read(&idx).unwrap();
    raw[FILE_HEADER_LEN + 4..FILE_HEADER_LEN + 8].copy_from_slice(&big.to_le_bytes());
    fs::write(&idx, &raw).unwrap();
    set_file_hwm(
        &BlockFilterTable::seg_dirs(&dir).body_path(0),
        u64::from(u32::MAX) + 1,
    );

    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(1));
    put_range(&t, 1, 2);
    assert!(BlockFilterTable::seg_dirs(&dir).body_path(1).is_file());
    assert_eq!(
        t.filters(Height(1), Height(2)).unwrap().unwrap(),
        vec![(vec![1u8; 4], slot(1)), (vec![2u8; 5], slot(2))]
    );
    assert_eq!(
        t.slots(Height(0), Height(2)).unwrap().unwrap(),
        (0..=2).map(slot).collect::<Vec<_>>(),
        "range read crosses the segment roll"
    );
    t.truncate_through(Some(Height(0))).unwrap();
    assert!(!BlockFilterTable::seg_dirs(&dir).body_path(1).exists());
    assert_eq!(t.next_height(), Height(1));
}

/// A crash inside a segment-pair create can keep one name of the newest
/// pair and lose the other. That pair holds no row, so open removes it and
/// keeps the pairs before it.
#[test]
fn open_drops_a_half_created_newest_pair() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    drop(dirs.create(1, 3).unwrap());
    fs::remove_file(dirs.body_path(1)).unwrap();
    let t = BlockFilterTable::open_or_create(&dir).expect("a half-created newest pair");
    assert_eq!(t.next_height(), Height(3));
    assert!(!dirs.idx_path(1).exists(), "the orphan idx stayed");
    assert_eq!(
        t.filters(Height(2), Height(2)).unwrap().unwrap(),
        vec![(vec![2u8; 5], slot(2))]
    );
}

/// One missing file in a pair before the newest is still corruption.
#[test]
fn open_refuses_a_missing_file_below_the_newest_pair() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    drop(dirs.create(1, 1).unwrap());
    drop(dirs.create(2, 1).unwrap());
    fs::remove_file(dirs.body_path(1)).unwrap();
    assert!(matches!(
        BlockFilterTable::open_or_create(&dir),
        Err(StoreError::Corrupt(m)) if m.contains("incomplete")
    ));
}

/// A pair create that fails leaves neither file, so the next roll can make
/// the same pair again.
#[test]
fn a_failed_pair_create_leaves_no_file() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    // A directory where the body file goes: the idx is made, the body is not.
    fs::create_dir_all(dirs.body_path(1)).unwrap();
    assert!(dirs.create(1, 0).is_err());
    assert!(!dirs.idx_path(1).exists(), "the failed create left its idx");
    fs::remove_dir(dirs.body_path(1)).unwrap();
    drop(dirs.create(1, 0).expect("the next create of the same pair"));
}

#[test]
fn put_rolls_mid_batch_when_the_next_start_passes_u32() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    drop(t);
    // Height 0 now ends 3 bytes before the u32 offset limit, so the next
    // put's first record still fits and the one after it must roll.
    let end = u32::MAX - 3;
    let len = end - FILE_HEADER_LEN as u32;
    let idx = BlockFilterTable::seg_dirs(&dir).idx_path(0);
    let mut raw = fs::read(&idx).unwrap();
    raw[FILE_HEADER_LEN + 4..FILE_HEADER_LEN + 8].copy_from_slice(&len.to_le_bytes());
    fs::write(&idx, &raw).unwrap();
    let body = BlockFilterTable::seg_dirs(&dir).body_path(0);
    set_file_hwm(&body, u64::from(end));
    // Sparse room past the logical end: a write near u32::MAX must not
    // fallocate the hole from offset 0.
    let f = fs::OpenOptions::new().write(true).open(&body).unwrap();
    f.set_len(u64::from(end) + (1 << 20)).unwrap();
    drop(f);

    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(1));
    put_range(&t, 1, 3);
    assert!(BlockFilterTable::seg_dirs(&dir).body_path(1).is_file());
    assert_eq!(t.filter(Height(1)).unwrap().unwrap().0, vec![1u8; 4]);
    assert_eq!(t.filter(Height(2)).unwrap().unwrap().0, vec![2u8; 5]);
    assert_eq!(t.filter(Height(3)).unwrap().unwrap().0, vec![3u8; 6]);
    assert_eq!(t.next_height(), Height(4));
}

#[test]
fn block_filter_slot_roundtrip_keeps_the_row() {
    let row = decode_slot(&encode_slot(9, 40, &slot(3)));
    assert_eq!(row.off, 9);
    assert_eq!(row.len, 40);
    assert_eq!(row.slot, slot(3));
}

/// A put that fails partway may leave one length durable and not the other.
/// The next put waits for reopen, which fits the files.
#[test]
#[cfg(debug_assertions)]
fn a_later_put_waits_for_reopen_after_a_failed_put() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    fail_next_idx_sync(&t);
    let err = t.put(&[BlockFilterRecord {
        height: Height(1),
        slot: slot(1),
        filter: &[1, 1, 1, 1],
    }]);
    assert!(err.is_err());
    let err = t.put(&[BlockFilterRecord {
        height: Height(1),
        slot: slot(1),
        filter: &[9, 9, 9, 9],
    }]);
    match err {
        Err(StoreError::Rejected(msg)) => {
            assert!(msg.contains("reopen"), "{msg}");
        }
        other => panic!("a failed put must refuse the next put, got {other:?}"),
    }
    assert_eq!(t.filter(Height(0)).unwrap().unwrap().0, vec![0u8; 3]);
    drop(t);
    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.next_height(), Height(1));
}

#[cfg(debug_assertions)]
fn fail_next_idx_sync(t: &BlockFilterTable) {
    let inner = t.inner.read().unwrap();
    let idx = &inner.segs.last().unwrap().idx;
    rbitcoin_store::io_handle::arm_fail_sync(idx.io_handle(), 1);
}

/// A failed put publishes nothing to readers: the failed height is not
/// readable, and the old prefix is.
#[test]
#[cfg(debug_assertions)]
fn a_failed_put_stays_out_of_the_read_watermark() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    fail_next_idx_sync(&t);
    assert!(t
        .put(&[BlockFilterRecord {
            height: Height(1),
            slot: slot(1),
            filter: &[1, 1, 1, 1],
        }])
        .is_err());
    assert_eq!(t.next_height(), Height(1));
    assert!(t.slot(Height(1)).unwrap().is_none());
    assert_eq!(t.slot(Height(0)).unwrap(), Some(slot(0)));
}

/// A crash can leave the body length durable and the idx length old. Open
/// cuts the body back to the idx, and the next put lands at the cut.
#[test]
#[cfg(debug_assertions)]
fn open_cuts_a_body_length_published_before_its_idx() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    let idx_path = {
        let inner = t.inner.read().unwrap();
        let idx = &inner.segs.last().unwrap().idx;
        rbitcoin_store::io_handle::arm_fail_flush(idx.io_handle(), 1);
        idx.path().to_path_buf()
    };
    let mut old_header = [0u8; FILE_HEADER_LEN];
    fs::File::open(&idx_path)
        .unwrap()
        .read_exact(&mut old_header)
        .unwrap();
    assert!(t
        .put(&[BlockFilterRecord {
            height: Height(1),
            slot: slot(1),
            filter: &[1, 1, 1, 1],
        }])
        .is_err());
    drop(t);
    // The idx length pwrite never reached the disk.
    let mut idx = fs::OpenOptions::new().write(true).open(&idx_path).unwrap();
    idx.write_all(&old_header).unwrap();
    drop(idx);
    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.next_height(), Height(1));
    t.put(&[BlockFilterRecord {
        height: Height(1),
        slot: slot(1),
        filter: &[7, 7],
    }])
    .unwrap();
    drop(t);
    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.filter(Height(1)).unwrap().unwrap().0, vec![7, 7]);
    assert_eq!(t.filter(Height(0)).unwrap().unwrap().0, vec![0u8; 3]);
}

/// A reader of a committed height does not wait for a put's syncs: the put
/// is paused inside its commit, after the syncs and before any length moves,
/// while the read runs.
#[cfg(debug_assertions)]
#[test]
fn a_committed_filter_reads_during_a_put() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    std::thread::scope(|scope| {
        let t = &t;
        let putting = scope.spawn(move || {
            rbitcoin_store::page_guard::set_commit_hook(Some(Box::new(move || {
                let _ = entered_tx.send(());
                let _ = release_rx.recv_timeout(std::time::Duration::from_secs(10));
                Ok(())
            })));
            t.put(&[BlockFilterRecord {
                height: Height(1),
                slot: slot(1),
                filter: &[1, 1, 1, 1],
            }])
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the put never reached its commit");
        let (tx, rx) = std::sync::mpsc::channel();
        scope.spawn(move || {
            let _ = tx.send(t.slot(Height(0)).map(|s| s.is_some()));
        });
        let got = rx.recv_timeout(std::time::Duration::from_secs(2));
        let _ = release_tx.send(());
        assert!(
            matches!(got, Ok(Ok(true))),
            "a committed read waited for the put's commit: {got:?}"
        );
        putting.join().unwrap().unwrap();
    });
    assert_eq!(t.next_height(), Height(2));
}

/// A put whose writes all go to a pair it rolls, and whose commit fails,
/// still makes the next put wait for reopen.
#[cfg(debug_assertions)]
#[test]
fn a_failed_put_into_a_rolled_pair_refuses_the_next_put() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    drop(t);
    // Record 0 now ends one past u32::MAX, so the next put rolls a pair.
    let big = u32::MAX - FILE_HEADER_LEN as u32 + 1;
    let idx = BlockFilterTable::seg_dirs(&dir).idx_path(0);
    let mut raw = fs::read(&idx).unwrap();
    raw[FILE_HEADER_LEN + 4..FILE_HEADER_LEN + 8].copy_from_slice(&big.to_le_bytes());
    fs::write(&idx, &raw).unwrap();
    set_file_hwm(
        &BlockFilterTable::seg_dirs(&dir).body_path(0),
        u64::from(u32::MAX) + 1,
    );
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    rbitcoin_store::page_guard::set_commit_hook(Some(Box::new(|| {
        Err(StoreError::Corrupt("injected commit failure"))
    })));
    let rec = |h: u32| BlockFilterRecord {
        height: Height(h),
        slot: slot(h),
        filter: &[1, 1, 1, 1],
    };
    assert!(t.put(&[rec(1)]).is_err());
    match t.put(&[rec(1)]) {
        Err(StoreError::Rejected(m)) => assert!(m.contains("reopen"), "{m}"),
        other => panic!("a put after a failed rolled put: {other:?}"),
    }
}

/// A repair of a committed filter waits for reopen after a failed put, like
/// the next put does.
#[test]
#[cfg(debug_assertions)]
fn repair_refuses_after_a_failed_put() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 0);
    let (bytes, got) = t.filter(Height(0)).unwrap().unwrap();
    fail_next_idx_sync(&t);
    assert!(t
        .put(&[BlockFilterRecord {
            height: Height(1),
            slot: slot(1),
            filter: &[1, 1, 1, 1],
        }])
        .is_err());
    assert!(matches!(
        t.repair_body(Height(0), &got, &bytes),
        Err(StoreError::Rejected(_))
    ));
}

/// A new table's `meta` and its two directories are durable before the
/// first put can return. A put syncs only the segment files and their own
/// directories.
#[cfg(all(unix, debug_assertions))]
#[test]
fn create_syncs_the_new_table_directories() {
    let dir = tmp_dir();
    rbitcoin_store::file::arm_fsync_dir_trace();
    let _t = BlockFilterTable::create(&dir).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    for want in [
        dir.to_path_buf(),
        BlockFilterTable::idx_dir(&dir),
        BlockFilterTable::body_dir(&dir),
    ] {
        assert!(
            synced.contains(&want),
            "{} was not synced, got {synced:?}",
            want.display()
        );
    }
}

/// A put is an append under the sector rule: bytes past the committed end,
/// synced, then the length. It arms no mark.
#[test]
fn a_put_arms_no_mark() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 3);
    assert!(!dir.join("guard").exists(), "a put armed a mark");
    assert_eq!(t.next_height(), Height(4));
}

/// A rolled segment pair is named by its directories before any put lands
/// in it: create syncs both directory entries.
#[cfg(all(unix, debug_assertions))]
#[test]
fn a_new_segment_pair_syncs_its_directories() {
    let dir = tmp_dir();
    let _t = BlockFilterTable::open_or_create(&dir).unwrap();
    let dirs = BlockFilterTable::seg_dirs(&dir);
    rbitcoin_store::file::arm_fsync_dir_trace();
    let _pair = dirs.create(1, 0).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    assert!(synced.contains(&dirs.idx), "idx dir not synced: {synced:?}");
    // Each header is synced before its name, so a surviving name opens.
    for (file, dir) in [
        (dirs.idx_path(1), &dirs.idx),
        (dirs.body_path(1), &dirs.body),
    ] {
        let file_at = synced.iter().position(|p| p == &file);
        let dir_at = synced.iter().position(|p| p == dir);
        assert!(
            matches!((file_at, dir_at), (Some(f), Some(d)) if f < d),
            "{} was not synced before its directory: {synced:?}",
            file.display()
        );
    }
    assert!(
        synced.contains(&dirs.body),
        "body dir not synced: {synced:?}"
    );
}

/// A first create that fails after its directories exist leaves no `meta`,
/// so the next `open_or_create` starts the table again instead of refusing a
/// table with no pairs.
#[test]
fn a_failed_first_create_is_retried_by_open_or_create() {
    let dir = tmp_dir();
    let dirs = BlockFilterTable::seg_dirs(&dir);
    // A directory where body 000000 goes: pair 000000 cannot be made.
    fs::create_dir_all(dirs.body_path(0)).unwrap();
    assert!(BlockFilterTable::create(&dir).is_err());
    assert!(
        !BlockFilterTable::meta_path(&dir).exists(),
        "a failed create left meta"
    );
    let t = BlockFilterTable::open_or_create(&dir).expect("the retried create");
    put_range(&t, 0, 1);
    drop(t);
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    assert_eq!(t.next_height(), Height(2));
}

/// A put that extends pair 0 and rolls pair 1 publishes pair 0's body length
/// before its idx length. A crash between them leaves pair 0's body past its
/// last row and pair 1 empty. Open fits pair 0 too, so a cut back to pair 0
/// and the next put land at the end of its last row. Small files stand in
/// for a 4 GiB pair 0: the fit does not depend on the roll offset.
#[test]
fn open_fits_the_last_populated_pair_before_an_empty_one() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    let end = HDR + (3 + 4 + 5);
    set_file_hwm(&dirs.body_path(0), end + 9);
    drop(dirs.create(1, 3).unwrap());

    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.next_height(), Height(3));
    put_range(&t, 3, 3);
    t.truncate_through(Some(Height(2))).unwrap();
    put_range(&t, 3, 4);
    drop(t);
    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.next_height(), Height(5));
    assert_eq!(
        t.filters(Height(2), Height(4)).unwrap().unwrap(),
        vec![
            (vec![2u8; 5], slot(2)),
            (vec![3u8; 6], slot(3)),
            (vec![4u8; 7], slot(4)),
        ]
    );
}

/// Overwrite the header of `path` with zeros: a name the directory
/// kept whose header never reached the disk.
fn zero_header(path: &Path) {
    let mut f = fs::OpenOptions::new().write(true).open(path).unwrap();
    f.write_all(&[0u8; FILE_HEADER_LEN]).unwrap();
}

/// A crash inside a pair create can keep both names of the newest pair while
/// a header did not persist. That pair holds no row: open drops it.
#[test]
fn open_drops_a_newest_pair_whose_header_never_landed() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    drop(dirs.create(1, 3).unwrap());
    zero_header(&dirs.body_path(1));
    let t = BlockFilterTable::open(&dir).expect("a newest pair with no header");
    assert_eq!(t.next_height(), Height(3));
    assert!(!dirs.idx_path(1).exists(), "the headerless pair stayed");
    drop(
        dirs.create(1, 3)
            .expect("the next roll makes the pair again"),
    );

    // A file shorter than a header is the same crash.
    fs::OpenOptions::new()
        .write(true)
        .open(dirs.idx_path(1))
        .unwrap()
        .set_len(0)
        .unwrap();
    drop(t);
    let t = BlockFilterTable::open(&dir).expect("a newest idx with no header");
    assert_eq!(t.next_height(), Height(3));
    assert!(!dirs.body_path(1).exists(), "the headerless pair stayed");
}

/// A bad header is still corruption when the pair has rows or a later pair
/// exists. Open refuses and deletes nothing.
#[test]
fn open_refuses_a_bad_header_in_a_pair_with_rows_or_a_successor() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    zero_header(&dirs.body_path(0));
    assert!(BlockFilterTable::open(&dir).is_err());
    assert!(dirs.idx_path(0).exists() && dirs.body_path(0).exists());

    let dir = tmp_dir();
    drop(BlockFilterTable::open_or_create(&dir).unwrap());
    let dirs = BlockFilterTable::seg_dirs(&dir);
    drop(dirs.create(1, 0).unwrap());
    zero_header(&dirs.body_path(0));
    assert!(BlockFilterTable::open(&dir).is_err());
    assert!(dirs.idx_path(0).exists() && dirs.body_path(0).exists());
}

/// A newest pair that lost one file is dropped only while the survivor shows
/// no length past its header (`open_drops_a_half_created_newest_pair`). A
/// survivor with rows means a file was lost after rows were published: open
/// refuses and deletes nothing, whichever file survived.
#[test]
fn open_refuses_a_newest_pair_missing_a_file_beside_published_rows() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    fs::remove_file(dirs.body_path(0)).unwrap();
    assert!(BlockFilterTable::open(&dir).is_err());
    assert!(dirs.idx_path(0).exists(), "open unlinked an idx with rows");

    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let dirs = BlockFilterTable::seg_dirs(&dir);
    fs::remove_file(dirs.idx_path(0)).unwrap();
    assert!(BlockFilterTable::open(&dir).is_err());
    assert!(dirs.body_path(0).exists(), "open unlinked a body with rows");
}

/// A presence check that fails is not a missing file: open returns the error
/// and keeps the pair. A regular file where the body directory goes makes
/// every body path fail to stat with `ENOTDIR`.
#[cfg(unix)]
#[test]
fn open_keeps_a_pair_whose_files_it_cannot_stat() {
    let dir = tmp_dir();
    let t = BlockFilterTable::open_or_create(&dir).unwrap();
    put_range(&t, 0, 2);
    drop(t);
    let body_dir = BlockFilterTable::body_dir(&dir);
    let aside = dir.join("body.aside");
    fs::rename(&body_dir, &aside).unwrap();
    fs::write(&body_dir, b"not a dir").unwrap();
    assert!(BlockFilterTable::open(&dir).is_err());
    let dirs = BlockFilterTable::seg_dirs(&dir);
    assert!(dirs.idx_path(0).exists(), "open unlinked a committed idx");
    fs::remove_file(&body_dir).unwrap();
    fs::rename(&aside, &body_dir).unwrap();
    let t = BlockFilterTable::open(&dir).unwrap();
    assert_eq!(t.next_height(), Height(3));
}

/// A filter put and a tweak put in one seal share three barriers. Both body
/// lengths are in their headers before either idx length is written.
#[cfg(debug_assertions)]
#[test]
fn a_filter_and_tweak_seal_is_three_barriers() {
    use rbitcoin_store::sp_tweaks::SpTweaksTable;
    let dir = tmp_dir();
    let filters = BlockFilterTable::open_or_create(&dir).unwrap();
    let tweaks = SpTweaksTable::create(&dir, Height(0)).unwrap();
    let filter_dirs = BlockFilterTable::seg_dirs(&dir);
    let paths = vec![
        filter_dirs.body_path(0),
        filter_dirs.idx_path(0),
        SpTweaksTable::body_dir(&dir).join("000000"),
        SpTweaksTable::idx_dir(&dir).join("000000"),
    ];
    let body = vec![1u8, 2, 3];
    let recs = [BlockFilterRecord {
        height: Height(0),
        slot: slot(0),
        filter: &body,
    }];
    let mut tweak = [0u8; 33];
    tweak[0] = 0x02;
    let row = [Some(tweak)];
    let items = [(Height(0), row.as_slice())];
    rbitcoin_store::file::arm_sync_barrier_trace(&paths);
    seal_filter_and_tweak_puts(&filters, &recs, &tweaks, &items).unwrap();
    let barriers = rbitcoin_store::file::take_sync_barrier_trace();
    assert_eq!(barriers.len(), 3, "{barriers:?}");
    let names = |synced: &[std::path::PathBuf]| {
        let mut names: Vec<_> = synced
            .iter()
            .map(|path| path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    };
    assert_eq!(barriers[0].synced.len(), 4, "{barriers:?}");
    assert!(
        barriers[0]
            .headers
            .iter()
            .all(|(_, len)| *len == FILE_HEADER_LEN as u64),
        "payload barrier wrote a length: {:?}",
        barriers[0].headers
    );
    assert_eq!(names(&barriers[1].synced).len(), 2);
    assert_eq!(names(&barriers[2].synced).len(), 2);
    let old = FILE_HEADER_LEN as u64;
    let idx_still_old = barriers[1]
        .headers
        .iter()
        .filter(|(path, _)| path.ends_with("000000") && path.to_string_lossy().contains(".idx"))
        .all(|(_, len)| *len == old);
    assert!(
        idx_still_old,
        "an idx length moved with the bodies: {barriers:?}"
    );
    assert_eq!(filters.next_height(), Height(1));
    assert_eq!(tweaks.next_height(), Height(1));
}
