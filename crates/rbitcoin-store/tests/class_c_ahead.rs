//! Class C flushes publish `store/guard/class_c` before the live pages.
//! Open restores that forward image before any slot read.
use rbitcoin_primitives::Fk;
use rbitcoin_store::header_table::block_header_hash;
use rbitcoin_store::header_table::HeaderRecord;
use rbitcoin_store::store::Store;
use rbitcoin_store::tx_table::{InputRecord, OutputRecord, TxRecord};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

fn tmp() -> PathBuf {
    let p = rbitcoin_store::testutil::fresh_path("class-c-ahead");
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn coinbase(h: u32) -> (TxRecord, Vec<InputRecord>, Vec<OutputRecord>) {
    let mut txid = [0u8; 32];
    txid[..4].copy_from_slice(&h.to_le_bytes());
    txid[31] = 0xD9;
    (
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
        vec![OutputRecord::unspent(50, vec![0x51])],
    )
}

fn hdr(prev: Fk, parent_hash: [u8; 32], h: u32, merkle: [u8; 32]) -> HeaderRecord {
    let version = 1i32;
    let timestamp = h + 1;
    let bits = 0x207f_ffff;
    let hash = if prev.is_null() {
        block_header_hash(version, &[0u8; 32], &merkle, timestamp, bits, h)
    } else {
        block_header_hash(version, &parent_hash, &merkle, timestamp, bits, h)
    };
    HeaderRecord {
        prev_fk: prev,
        version,
        timestamp,
        bits,
        nonce: h,
        merkle_root: merkle,
        hash,
        size: 0,
        weight: 0,
    }
}

/// Zero the first bytes and the 4 KiB block that holds the 512th range
/// onwards. The write-ahead image is the only copy left.
fn zero_header_and_block(path: &Path) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&[0u8; 4096]).unwrap();
    file.seek(SeekFrom::Start(4096)).unwrap();
    file.write_all(&[0u8; 4096]).unwrap();
    file.sync_all().unwrap();
    let mut check = [1u8; 4096];
    file.seek(SeekFrom::Start(0)).unwrap();
    file.read_exact(&mut check).unwrap();
    assert_eq!(check, [0u8; 4096]);
}

/// The Class C group pause is one process-wide slot. Tests that arm it hold
/// this lock from arm to release, so two of them cannot share the slot.
#[cfg(debug_assertions)]
fn pause_slot() -> std::sync::MutexGuard<'static, ()> {
    static SLOT: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SLOT.lock().unwrap_or_else(|e| e.into_inner())
}

/// Releases the group pause on drop, including on unwind.
#[cfg(debug_assertions)]
struct ReleasePause;

#[cfg(debug_assertions)]
impl Drop for ReleasePause {
    fn drop(&mut self) {
        rbitcoin_store::chain::release_class_c_group_pause();
    }
}

fn class_c_mark(dir: &Path) -> PathBuf {
    dir.join(rbitcoin_store::page_guard::CLASS_C_MARK)
}

#[test]
fn crash_between_header_txs_first_and_count_is_one_mark() {
    let dir = tmp();
    let body = dir.join("header_txs_first.body");
    {
        let t = rbitcoin_store::HeaderTxsTable::create(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        t.put_range(&write, Fk(1), Fk(1), 1).unwrap();
        t.flush().unwrap();
        t.put_range(&write, Fk(1), Fk(5), 3).unwrap();
        rbitcoin_store::chain::fail_next_header_txs_flush_after_first(&body);
        assert!(t.flush().is_err());
        assert!(
            class_c_mark(&dir).is_file(),
            "first and count share one mark"
        );
    }
    zero_header_and_block(&dir.join("header_txs_first.body"));
    zero_header_and_block(&dir.join("header_txs_count.body"));
    let t = rbitcoin_store::HeaderTxsTable::open(&dir).unwrap();
    assert_eq!(
        (t.first.get(0).unwrap(), t.count.get(0).unwrap()),
        (5, 3),
        "open observed a split first/count pair"
    );
    assert!(!class_c_mark(&dir).exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn open_restores_the_pair_when_a_retry_rewrites_one_sidecar() {
    let dir = tmp();
    let body = dir.join("header_txs_first.body");
    {
        let t = rbitcoin_store::HeaderTxsTable::create(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        t.put_range(&write, Fk(1), Fk(1), 1).unwrap();
        t.flush().unwrap();
        t.put_range(&write, Fk(1), Fk(5), 3).unwrap();
        rbitcoin_store::chain::fail_next_header_txs_flush_after_first(&body);
        assert!(t.flush().is_err());
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&body);
        assert!(t.flush().is_err());
    }
    let t = rbitcoin_store::HeaderTxsTable::open(&dir).unwrap();
    assert_eq!(
        (t.first.get(0).unwrap(), t.count.get(0).unwrap()),
        (5, 3),
        "open kept first 5 beside count 1"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_txs_flush_dirsyncs_the_pair_once() {
    let dir = tmp();
    let t = rbitcoin_store::HeaderTxsTable::create(&dir).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    t.put_range(&write, Fk(1), Fk(1), 1).unwrap();
    rbitcoin_store::file::arm_fsync_dir_trace();
    t.flush().unwrap();
    let dirs = rbitcoin_store::file::take_fsynced_dirs();
    #[cfg(unix)]
    {
        let guard = dir.join("guard");
        assert_eq!(
            dirs.iter().filter(|p| p.as_path() == guard).count(),
            2,
            "the pair should sync guard/ once after the sidecars and once after the unlinks, got {dirs:?}"
        );
        assert!(
            dirs.iter().any(|p| p == &dir),
            "creating guard/ must sync the store directory, got {dirs:?}"
        );
    }
    // Windows `fsync_parent_dir` is a no-op, so the trace stays empty.
    #[cfg(not(unix))]
    drop(dirs);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn torn_header_txs_block_restores_from_ahead_image() {
    let dir = tmp();
    let (h600, h601);
    {
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let mut parent_hash = [0u8; 32];
        let mut prev = Fk::NULL;
        let mut last = None;
        for h in 0..600u32 {
            let (rec, ins, outs) = coinbase(h);
            let tx = s
                .put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
                .unwrap()[0];
            let txid = s.txs.body_txid(tx).unwrap();
            let header = hdr(prev, parent_hash, h, txid);
            parent_hash = header.hash;
            let hfk = s.put_header(&header).unwrap();
            s.header_txs.put_range(&write, hfk, tx, 1).unwrap();
            prev = hfk;
            last = Some(hfk);
        }
        s.header_txs.flush().unwrap();
        h600 = last.unwrap();
        assert!(s.header_txs.has_body(h600).unwrap());

        // The 601st range starts in the block that holds ranges 510..=599.
        let (rec, ins, outs) = coinbase(600);
        let tx = s
            .put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
            .unwrap()[0];
        let txid = s.txs.body_txid(tx).unwrap();
        let header = hdr(prev, parent_hash, 600, txid);
        let hfk = s.put_header(&header).unwrap();
        s.header_txs.put_range(&write, hfk, tx, 1).unwrap();
        h601 = hfk;
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(
            &dir.join("header_txs_first.body"),
        );
        assert!(
            s.header_txs.flush().is_err(),
            "the failpoint must stop before the payload flush"
        );
        assert!(class_c_mark(&dir).is_file());
    }

    zero_header_and_block(&dir.join("header_txs_first.body"));
    zero_header_and_block(&dir.join("header_txs_count.body"));
    let s = Store::open_tiny(&dir).unwrap();
    assert!(
        s.header_txs.has_body(h600).unwrap(),
        "the committed 600th range was lost"
    );
    assert!(
        s.header_txs.has_body(h601).unwrap(),
        "the write-ahead 601st range was lost"
    );
    assert_eq!(
        s.header_txs.get_range(h601).unwrap().map(|(f, n)| (f.0, n)),
        Some((601, 1))
    );
    assert_eq!(
        s.header_txs
            .get_range(Fk(1))
            .unwrap()
            .map(|(f, n)| (f.0, n)),
        Some((1, 1)),
        "the page-zero range was not restored"
    );
    assert!(!class_c_mark(&dir).exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn leftover_ahead_pages_survive_a_later_write() {
    let dir = tmp();
    {
        let s = Store::create_tiny(&dir).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        let mut parent_hash = [0u8; 32];
        let mut prev = Fk::NULL;
        for h in 0..1100u32 {
            let (rec, ins, outs) = coinbase(h);
            let tx = s
                .put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
                .unwrap()[0];
            let txid = s.txs.body_txid(tx).unwrap();
            let header = hdr(prev, parent_hash, h, txid);
            parent_hash = header.hash;
            let hfk = s.put_header(&header).unwrap();
            s.header_txs.put_range(&write, hfk, tx, 1).unwrap();
            prev = hfk;
            if h == 599 {
                s.header_txs.flush().unwrap();
            }
        }
        rbitcoin_store::chain::fail_next_array_flush_after_payload(
            &dir.join("header_txs_first.body"),
        );
        assert!(
            s.header_txs.flush().is_err(),
            "the failpoint must leave the sidecar after a successful payload"
        );
        assert!(class_c_mark(&dir).is_file());

        let (rec, ins, outs) = coinbase(1100);
        let tx = s
            .put_tx_full_batch_indexed(&[(rec, ins, outs)], true)
            .unwrap()[0];
        let txid = s.txs.body_txid(tx).unwrap();
        let header = hdr(prev, parent_hash, 1100, txid);
        let hfk = s.put_header(&header).unwrap();
        s.header_txs.put_range(&write, hfk, tx, 1).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(
            &dir.join("header_txs_first.body"),
        );
        assert!(
            s.header_txs.flush().is_err(),
            "the later write must replace the sidecar"
        );
    }

    let first = dir.join("header_txs_first.body");
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&first)
        .unwrap();
    file.seek(SeekFrom::Start(4096)).unwrap();
    file.write_all(&[0u8; 4096]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let s = Store::open_tiny(&dir).unwrap();
    assert!(
        s.header_txs.has_body(Fk(600)).unwrap(),
        "the committed range on the first page was lost"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The `confirmed[]` sidecar's rename must be durable before the payload
/// write it guards, and its unlink after it: one directory sync each.
#[test]
fn confirmed_flush_dirsyncs_the_sidecar_before_the_payload() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("confirmed-dirsync").unwrap();
    let path = dir.path.join("confirmed.body");
    let t = ConfirmedTable::create(&dir.path).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    t.set(&write, Height(0), Fk(1)).unwrap();
    rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
    rbitcoin_store::file::arm_fsync_dir_trace();
    assert!(t.flush().is_err(), "the failpoint stops before the payload");
    let before_payload = rbitcoin_store::file::take_fsynced_dirs();
    rbitcoin_store::file::arm_fsync_dir_trace();
    t.flush().unwrap();
    let full = rbitcoin_store::file::take_fsynced_dirs();
    #[cfg(unix)]
    {
        let guard = dir.path.join("guard");
        assert!(
            before_payload.iter().any(|p| p == &guard),
            "the sidecar rename must reach guard/ before the payload, got {before_payload:?}"
        );
        assert!(
            before_payload.iter().any(|p| p == dir.path()),
            "creating guard/ must sync the store directory, got {before_payload:?}"
        );
        assert_eq!(
            full.iter().filter(|p| p.as_path() == guard).count(),
            2,
            "one sync after the sidecar and one after the unlink, got {full:?}"
        );
    }
    // Windows `fsync_parent_dir` is a no-op, so the trace stays empty.
    #[cfg(not(unix))]
    drop((before_payload, full));
}

/// Every `confirmed[]` flush rewrites the length in page 0 and the block that
/// holds the previous tip. A tear of either must come back from the image.
#[test]
fn torn_confirmed_page_zero_restores_from_ahead_image() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("confirmed-ahead").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        for h in 0..600u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        t.flush().unwrap();
        // Height 600 lands in the block that holds heights 510..=599.
        t.set(&write, Height(600), Fk(601)).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
        assert!(
            t.flush().is_err(),
            "the failpoint must stop before the payload flush"
        );
        assert!(class_c_mark(&dir.path).is_file());
    }

    zero_header_and_block(&path);
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.tip_height(), Some(Height(600)));
    assert_eq!(t.get(Height(0)).unwrap(), Some(Fk(1)), "page zero was lost");
    assert_eq!(
        t.get(Height(599)).unwrap(),
        Some(Fk(600)),
        "the committed previous tip was lost"
    );
    assert_eq!(t.get(Height(600)).unwrap(), Some(Fk(601)));
    assert!(!class_c_mark(&dir.path).exists());
}

/// A disconnect to a length that ends on a 4 KiB boundary writes no slot,
/// but the flush still rewrites the length in page 0.
#[test]
fn torn_page_zero_after_a_block_aligned_shrink_restores() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("confirmed-aligned").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        for h in 0..512u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        t.flush().unwrap();
        // 16 B header + 510 slots is exactly one 4 KiB block.
        t.disconnect_tip(&write, Height(511)).unwrap();
        t.disconnect_tip(&write, Height(510)).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
        assert!(t.flush().is_err(), "the failpoint must stop the flush");
        assert!(class_c_mark(&dir.path).is_file());
    }

    zero_header_and_block(&path);
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.tip_height(), Some(Height(509)));
    assert_eq!(t.get(Height(0)).unwrap(), Some(Fk(1)));
    assert_eq!(t.get(Height(509)).unwrap(), Some(Fk(510)));
}

/// A shrink to an empty table writes no slot, but the flush still rewrites
/// the length in page 0.
#[test]
fn torn_page_zero_after_a_shrink_to_empty_restores() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("confirmed-empty").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        t.set(&write, Height(0), Fk(1)).unwrap();
        t.flush().unwrap();
        t.disconnect_tip(&write, Height(0)).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
        assert!(t.flush().is_err(), "the failpoint must stop the flush");
        assert!(class_c_mark(&dir.path).is_file());
    }

    zero_header_and_block(&path);
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.tip_height(), None);
    assert!(!class_c_mark(&dir.path).exists());
}

/// A crash after the payload landed and before the sidecar was removed:
/// open replays the same pages and removes the sidecar.
#[test]
fn confirmed_ahead_left_after_the_payload_replays_the_same_state() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("confirmed-after").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        for h in 0..520u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        rbitcoin_store::chain::fail_next_array_flush_after_payload(&path);
        assert!(
            t.flush().is_err(),
            "the failpoint must stop before the sidecar is removed"
        );
        assert!(class_c_mark(&dir.path).is_file());
    }

    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.tip_height(), Some(Height(519)));
    assert_eq!(t.get(Height(0)).unwrap(), Some(Fk(1)));
    assert_eq!(t.get(Height(519)).unwrap(), Some(Fk(520)));
    assert!(!class_c_mark(&dir.path).exists());
}

#[test]
fn one_header_txs_failpoint_does_not_clear_another_path() {
    let a = rbitcoin_store::testutil::TempDir::labeled("ahead-a").unwrap();
    let b = rbitcoin_store::testutil::TempDir::labeled("ahead-b").unwrap();
    let ta = rbitcoin_store::HeaderTxsTable::create(&a.path).unwrap();
    let tb = rbitcoin_store::HeaderTxsTable::create(&b.path).unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    ta.put_range(&write, Fk(1), Fk(1), 1).unwrap();
    tb.put_range(&write, Fk(1), Fk(1), 1).unwrap();
    let pa = a.path.join("header_txs_first.body");
    let pb = b.path.join("header_txs_first.body");
    rbitcoin_store::chain::fail_next_array_flush_after_ahead(&pa);
    rbitcoin_store::chain::fail_next_array_flush_after_ahead(&pb);
    assert!(tb.flush().is_err(), "path b was armed");
    assert!(
        ta.flush().is_err(),
        "arming path b cleared the failpoint for path a"
    );
    rbitcoin_store::chain::fail_next_array_flush_after_payload(&pa);
    rbitcoin_store::chain::fail_next_array_flush_after_payload(&pb);
    assert!(tb.flush().is_err(), "path b payload failpoint was armed");
    assert!(
        ta.flush().is_err(),
        "arming path b cleared the payload failpoint for path a"
    );
}

#[test]
fn one_page_flush_restores_a_mixed_committed_slot() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("c-one-page").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        for h in 0..80u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        t.flush().unwrap();
        t.set(&write, Height(40), Fk(9_001)).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
        assert!(t.flush().is_err());
        assert!(class_c_mark(&dir.path).is_file());
    }
    zero_header_and_block(&path);
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.get(Height(39)).unwrap(), Some(Fk(40)));
    assert_eq!(t.get(Height(40)).unwrap(), Some(Fk(9_001)));
    assert_eq!(t.get(Height(41)).unwrap(), Some(Fk(42)));
    assert_eq!(t.tip_height(), Some(Height(79)));
}

#[test]
fn flush_from_first_slot_restores_a_mixed_committed_slot() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("c-from-first").unwrap();
    let path = dir.path.join("confirmed.body");
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        let write = rbitcoin_store::ClassCWrite::for_test();
        for h in 0..600u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        t.flush().unwrap();
        t.set(&write, Height(0), Fk(9_002)).unwrap();
        rbitcoin_store::chain::fail_next_array_flush_after_ahead(&path);
        assert!(t.flush().is_err());
        assert!(class_c_mark(&dir.path).is_file());
    }
    zero_header_and_block(&path);
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.get(Height(0)).unwrap(), Some(Fk(9_002)));
    assert_eq!(t.get(Height(1)).unwrap(), Some(Fk(2)));
    assert_eq!(t.get(Height(599)).unwrap(), Some(Fk(600)));
    assert_eq!(t.tip_height(), Some(Height(599)));
}

/// `HeaderTxsTable::flush` takes the write lock itself. This pause is inside
/// the grouped flush `flush_header_archive` used to call without that lock.
#[cfg(debug_assertions)]
#[test]
fn header_txs_grouped_flush_holds_the_write_lock() {
    use std::sync::{mpsc, Arc};
    use std::time::Duration;

    let dir = tmp();
    let store = Arc::new(Store::create_tiny(&dir).unwrap());
    let write = rbitcoin_store::ClassCWrite::for_test();
    store.header_txs.put_range(&write, Fk(1), Fk(1), 1).unwrap();

    let _slot = pause_slot();
    rbitcoin_store::chain::arm_class_c_group_pause(&dir);
    let _release = ReleasePause;
    let flushing = Arc::clone(&store);
    let flush = std::thread::spawn(move || flushing.flush_header_archive());
    assert!(
        rbitcoin_store::chain::wait_class_c_group_paused(Duration::from_secs(5)),
        "flush_header_archive did not pause between header_txs columns"
    );
    let (tx, rx) = mpsc::channel();
    let putting = Arc::clone(&store);
    let put = std::thread::spawn(move || {
        let write = rbitcoin_store::ClassCWrite::for_test();
        putting
            .header_txs
            .put_range(&write, Fk(2), Fk(2), 1)
            .unwrap();
        tx.send(()).unwrap();
    });
    assert!(
        rx.recv_timeout(Duration::from_millis(400)).is_err(),
        "put_range finished while flush_header_archive was between columns"
    );
    rbitcoin_store::chain::release_class_c_group_pause();
    flush.join().unwrap().unwrap();
    put.join().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A slot set after the page images and before the live write dirties a page
/// the mark does not hold. The flush writes only the span it imaged, and the
/// late slot stays dirty for the next flush.
#[cfg(debug_assertions)]
#[test]
fn flush_writes_only_the_pages_it_imaged() {
    use rbitcoin_primitives::Height;
    use rbitcoin_store::chain::ConfirmedTable;
    use rbitcoin_store::file::TracedWrite;
    use std::time::Duration;

    let dir = rbitcoin_store::testutil::TempDir::labeled("c-late-slot").unwrap();
    let path = dir.path.join("confirmed.body");
    let write = rbitcoin_store::ClassCWrite::for_test();
    {
        let t = ConfirmedTable::create(&dir.path).unwrap();
        for h in 0..2000u32 {
            t.set(&write, Height(h), Fk(u64::from(h) + 1)).unwrap();
        }
        t.flush().unwrap();
        // Slot 1500 sits on page 2. Page 1 holds slots 510..1021.
        t.set(&write, Height(1500), Fk(9_001)).unwrap();

        let _slot = pause_slot();
        rbitcoin_store::chain::arm_class_c_group_pause(&dir.path);
        let _release = ReleasePause;
        std::thread::scope(|scope| {
            scope.spawn(|| {
                // Releases the paused flush even if an assert below panics.
                let _release = ReleasePause;
                assert!(
                    rbitcoin_store::chain::wait_class_c_group_paused(Duration::from_secs(5)),
                    "the flush did not pause after its page images"
                );
                let write = rbitcoin_store::ClassCWrite::for_test();
                t.set(&write, Height(600), Fk(9_002)).unwrap();
            });
            rbitcoin_store::file::arm_write_trace();
            t.flush().unwrap();
            let trace = rbitcoin_store::file::take_write_trace();
            for w in trace {
                if let TracedWrite::Write {
                    path: p,
                    offset,
                    len,
                } = w
                {
                    if p == path {
                        assert!(
                            offset + len <= 4096 || offset >= 8192,
                            "the flush wrote page 1, which its mark does not hold: {offset}+{len}"
                        );
                    }
                }
            }
        });
        t.flush().unwrap();
    }
    let t = ConfirmedTable::open(&dir.path).unwrap();
    assert_eq!(t.get(Height(600)).unwrap(), Some(Fk(9_002)));
    assert_eq!(t.get(Height(1500)).unwrap(), Some(Fk(9_001)));
}

/// An fd-only `header_txs` pair lands under one mark. A failure after the
/// `first` column is written and before `count` is must not leave the pair
/// split: both forward images are armed by then, so reopen reads the new pair.
#[cfg(debug_assertions)]
#[test]
fn fd_only_header_txs_pair_is_one_mark() {
    use rbitcoin_store::chain::HeaderTxsTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("c-fd-pair").unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    {
        let t = HeaderTxsTable::create(&dir.path).unwrap();
        t.put_range(&write, Fk(1), Fk(1), 1).unwrap();
        t.flush().unwrap();
    }
    {
        let t = rbitcoin_store::array_table::with_class_c_inram_max_bytes(0, || {
            HeaderTxsTable::open(&dir.path).unwrap()
        });
        rbitcoin_store::chain::fail_next_fd_write_before_live(
            &dir.path.join("header_txs_count.body"),
        );
        assert!(t.put_range(&write, Fk(1), Fk(5), 3).is_err());
    }
    assert!(rbitcoin_store::page_guard::restore_store_group(
        &dir.path,
        rbitcoin_store::page_guard::CLASS_C_MARK,
    )
    .unwrap());
    let t = HeaderTxsTable::open(&dir.path).unwrap();
    assert_eq!(t.get_range(Fk(1)).unwrap(), Some((Fk(5), 3)));
}

/// Each `header_txs` column picks RAM or fd from its own length. A pair whose
/// lengths straddle the RAM cap would open as one fd column and one RAM
/// column, and no single mark covers a write to that pair. Open refuses it.
#[test]
fn header_txs_pair_on_two_tiers_refuses_open() {
    use rbitcoin_store::chain::HeaderTxsTable;
    let dir = rbitcoin_store::testutil::TempDir::labeled("c-mixed-pair").unwrap();
    let write = rbitcoin_store::ClassCWrite::for_test();
    {
        let t = HeaderTxsTable::create(&dir.path).unwrap();
        t.put_range(&write, Fk(1), Fk(1), 1).unwrap();
        t.first.set_zero_gap(&write, 1, 2).unwrap();
        t.flush().unwrap();
        assert_eq!((t.first.len(), t.count.len()), (2, 1));
    }
    let opened = rbitcoin_store::array_table::with_class_c_inram_max_bytes(8, || {
        HeaderTxsTable::open(&dir.path)
    });
    match opened {
        Err(rbitcoin_store::error::StoreError::Corrupt(m)) => {
            assert!(m.contains("header_txs"), "{m}")
        }
        Err(other) => panic!("wrong refusal: {other}"),
        Ok(_) => panic!("open accepted a header_txs pair split across tiers"),
    }
}
