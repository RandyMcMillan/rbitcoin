use rbitcoin_primitives::Fk;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::FILE_HEADER_LEN;
use rbitcoin_store::spender_table::*;
use rbitcoin_store::Store;

#[test]
fn append_get_chain() {
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-spender-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let t = SpenderTable::create(&dir).unwrap();
    assert!(dir.join("spent.ovf").exists(), "schema 17 name");
    assert!(
        !dir.join("spenders.body").exists(),
        "legacy filename must not be created"
    );
    let a = t.append(Fk(10), 3, Fk::NULL).unwrap();
    let b = t.append(Fk(11), 4, a).unwrap();
    assert_eq!(t.get(a).unwrap(), (Fk(10), 3, Fk::NULL));
    assert_eq!(t.get(b).unwrap(), (Fk(11), 4, a));
    assert_eq!(t.count(), 2);
    assert!(matches!(
        t.append(Fk::NULL, 0, Fk::NULL),
        Err(StoreError::InvalidFk)
    ));
    assert!(matches!(t.get(Fk::NULL), Err(StoreError::InvalidFk)));
    assert!(matches!(t.get(Fk(99)), Err(StoreError::NotFound)));
    let freeze_dir = rbitcoin_store::testutil::TempDir::labeled("spender-freeze").unwrap();
    let freeze = Store::create_tiny(freeze_dir.path()).unwrap();
    let g = freeze.hold_utxo_view();
    t.flush(&SpenderFreeze::new(&g)).unwrap();
    t.flush_async().unwrap();
    drop(t);
    let t = SpenderTable::open(&dir).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.get(a).unwrap(), (Fk(10), 3, Fk::NULL));
    let dir2 = dir.with_extension("empty");
    let _ = std::fs::remove_dir_all(&dir2);
    std::fs::create_dir_all(&dir2).unwrap();
    let t2 = SpenderTable::open(&dir2).unwrap();
    assert_eq!(t2.count(), 0);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

#[test]
fn open_rejects_bad_body_size() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spender-bad-size").unwrap();
    {
        let s = Store::create_tiny(dir.path()).unwrap();
        s.spenders.append(Fk(1), 0, Fk::NULL).unwrap();
        let g = s.hold_utxo_view();
        s.spenders.flush(&SpenderFreeze::new(&g)).unwrap();
    }
    // Shrink below HWM so open clamps logical_len to a non-multiple of 16.
    let body = dir.path().join("spent.ovf");
    std::fs::OpenOptions::new()
        .write(true)
        .open(&body)
        .unwrap()
        .set_len((FILE_HEADER_LEN + 3) as u64)
        .unwrap();
    assert!(matches!(
        SpenderTable::open(dir.path()),
        Err(StoreError::Corrupt(_))
    ));
}

#[test]
fn open_renames_leftover_spenders_body_to_spent_ovf() {
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-spender-legacy-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    {
        let t = SpenderTable::create(&dir).unwrap();
        t.append(Fk(7), 9, Fk::NULL).unwrap();
        let freeze_dir =
            rbitcoin_store::testutil::TempDir::labeled("spender-legacy-freeze").unwrap();
        let freeze = Store::create_tiny(freeze_dir.path()).unwrap();
        let g = freeze.hold_utxo_view();
        t.flush(&SpenderFreeze::new(&g)).unwrap();
    }
    std::fs::rename(dir.join("spent.ovf"), dir.join("spenders.body")).unwrap();
    assert!(!dir.join("spent.ovf").exists());
    let t = SpenderTable::open(&dir).unwrap();
    assert!(dir.join("spent.ovf").exists());
    assert!(!dir.join("spenders.body").exists());
    assert_eq!(t.count(), 1);
    assert_eq!(t.get(Fk(1)).unwrap(), (Fk(7), 9, Fk::NULL));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn append_fk_or_vin_overflow_is_corrupt() {
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-spender-ovf-cap-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let t = SpenderTable::create(&dir).unwrap();
    match t.append(Fk(1u64 << 40), 0, Fk::NULL) {
        Err(StoreError::Corrupt(m)) => assert!(m.contains("u40"), "{m}"),
        other => panic!("expected u40 Corrupt, got {other:?}"),
    }
    match t.append(Fk(1), 1u32 << 16, Fk::NULL) {
        Err(StoreError::Corrupt(m)) => assert!(m.contains("u16"), "{m}"),
        other => panic!("expected u16 Corrupt, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn spender_get_reads_the_packed_record() {
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-spender-fields-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let t = SpenderTable::create(&dir).unwrap();
    let next = Fk(0x1111_2222_3333_4444);
    let id = t.append(Fk(0x0102_0304), 0x0107, next).unwrap();
    assert_eq!(t.get(id).unwrap(), (Fk(0x0102_0304), 0x0107, next));
    match t.get(Fk(99)) {
        Err(StoreError::NotFound) => {}
        other => panic!("missing spender must be NotFound, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crash between flushes reopens at the last flushed length. Bytes past it,
/// torn or not, are ignored, and the next append takes the next id.
#[test]
fn a_crash_between_flushes_reopens_at_the_flushed_length() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spent-ovf-crash").unwrap();
    let path = dir.path().to_path_buf();
    let committed = 300u64;
    let kept;
    {
        let s = Store::create_tiny(&path).unwrap();
        for i in 1..=committed {
            s.spenders.append(Fk(i), i as u32, Fk::NULL).unwrap();
        }
        {
            let g = s.hold_utxo_view();
            s.spenders.flush(&SpenderFreeze::new(&g)).unwrap();
        }
        kept = s.spenders.get(Fk(committed)).unwrap();
        s.spenders.append(Fk(9), 1, Fk::NULL).unwrap();
        s.spenders.append(Fk(9), 2, Fk::NULL).unwrap();
        // The process dies here: nothing flushes on the way out.
        std::mem::forget(s);
    }
    let end = SpenderTable::offset(committed + 1);
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(path.join("spent.ovf"))
            .unwrap();
        std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(end)).unwrap();
        std::io::Write::write_all(&mut file, &[0xffu8; 2 * SPENDER_RECORD_LEN]).unwrap();
        file.sync_all().unwrap();
    }
    let s = Store::open_tiny(&path).unwrap();
    assert_eq!(s.spenders.count(), committed);
    assert_eq!(s.spenders.get(Fk(committed)).unwrap(), kept);
    assert!(matches!(
        s.spenders.get(Fk(committed + 1)),
        Err(StoreError::NotFound)
    ));
    let next = s.spenders.append(Fk(5), 5, Fk(committed)).unwrap();
    assert_eq!(next, Fk(committed + 1));
    assert_eq!(s.spenders.get(next).unwrap(), (Fk(5), 5, Fk(committed)));
}

/// An append writes past the durable end and arms no mark. Only a flush
/// publishes the length, after the nodes are synced.
#[test]
fn an_append_arms_no_mark_and_waits_for_flush_to_publish() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spent-ovf-append-order").unwrap();
    let path = dir.path().to_path_buf();
    let s = Store::create_tiny(&path).unwrap();
    let fk = s.spenders.append(Fk(7), 1, Fk::NULL).unwrap();
    assert_eq!(s.spenders.get(fk).unwrap(), (Fk(7), 1, Fk::NULL));
    assert!(
        !path.join("guard/spent.ovf").exists(),
        "an append arms no mark"
    );
    assert_eq!(
        SpenderTable::open(&path).unwrap().count(),
        0,
        "the durable length waits for a flush"
    );
    {
        let g = s.hold_utxo_view();
        s.spenders.flush(&SpenderFreeze::new(&g)).unwrap();
    }
    let reopened = SpenderTable::open(&path).unwrap();
    assert_eq!(reopened.count(), 1);
    assert_eq!(reopened.get(fk).unwrap(), (Fk(7), 1, Fk::NULL));
}

/// A flush whose node sync fails publishes no length, and the latch keeps
/// a retry from publishing one.
#[test]
fn a_failed_node_sync_publishes_no_length() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spent-ovf-sync-first").unwrap();
    let path = dir.path().to_path_buf();
    let s = Store::create_tiny(&path).unwrap();
    s.spenders.append(Fk(7), 1, Fk::NULL).unwrap();
    rbitcoin_store::io_handle::arm_fail_sync(s.spenders.body.io_handle(), 1);
    let g = s.hold_utxo_view();
    let freeze = SpenderFreeze::new(&g);
    assert!(s.spenders.flush(&freeze).is_err());
    assert!(s.spenders.flush(&freeze).is_err(), "the latch holds");
    assert_eq!(SpenderTable::open(&path).unwrap().count(), 0);
}

/// An append that lands after the node sync and before the length write is
/// not in that sync. The flush publishes only the length it synced; the
/// later node waits for the next flush.
#[cfg(debug_assertions)]
#[test]
fn an_append_during_a_flush_stays_out_of_its_length() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spent-ovf-race").unwrap();
    let path = dir.path().to_path_buf();
    let s = Store::create_tiny(&path).unwrap();
    s.spenders.append(Fk(7), 1, Fk::NULL).unwrap();
    rbitcoin_store::spender_table::set_after_node_sync(Some(Box::new(|t: &SpenderTable| {
        t.append(Fk(8), 1, Fk::NULL).unwrap();
    })));
    {
        let g = s.hold_utxo_view();
        s.spenders.flush(&SpenderFreeze::new(&g)).unwrap();
    }
    assert_eq!(s.spenders.count(), 2);
    let header_len = |p: &std::path::Path| {
        let raw = std::fs::read(p.join("spent.ovf")).unwrap();
        u64::from_le_bytes(raw[8..16].try_into().unwrap())
    };
    assert_eq!(
        header_len(&path),
        SpenderTable::offset(2),
        "the flush published a node its sync did not cover"
    );
    {
        let g = s.hold_utxo_view();
        s.spenders.flush(&SpenderFreeze::new(&g)).unwrap();
    }
    assert_eq!(header_len(&path), SpenderTable::offset(3));
}

/// The length sync is the last step of a flush. When it fails, the flush
/// errs and the latch refuses the next one.
#[cfg(debug_assertions)]
#[test]
fn a_failed_length_sync_latches_the_flush() {
    let dir = rbitcoin_store::testutil::TempDir::labeled("spent-ovf-len-sync").unwrap();
    let path = dir.path().to_path_buf();
    let s = Store::create_tiny(&path).unwrap();
    s.spenders.append(Fk(7), 1, Fk::NULL).unwrap();
    // The node sync passes; the length sync after it fails.
    rbitcoin_store::io_handle::arm_fail_sync_after(s.spenders.body.io_handle(), 1, 1);
    let g = s.hold_utxo_view();
    let freeze = SpenderFreeze::new(&g);
    assert!(s.spenders.flush(&freeze).is_err());
    assert!(s.spenders.flush(&freeze).is_err(), "the latch holds");
}
