use rbitcoin_primitives::Fk;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::testutil::TempDir;
use rbitcoin_store::txstat::*;

fn tiny(fee: u64, base: u32, wit: u32) -> TxStatRow {
    TxStatRow {
        fee_sat: fee,
        base,
        wit_extra: wit,
    }
}

#[test]
fn txstat_blob_longer_than_the_file_is_corrupt() {
    let err = checked_blob_len(1 << 30, 32).expect_err("huge len");
    match err {
        StoreError::Corrupt(msg) => assert!(msg.contains("txstat"), "{msg}"),
        other => panic!("{other:?}"),
    }
    assert_eq!(checked_blob_len(4, 32).unwrap(), 4);
}

#[test]
fn create_empty_then_extend_zeros() {
    let dir = TempDir::labeled("txstat-extend").unwrap();
    let t = TxStat::create(&dir).unwrap();
    assert_eq!(t.count(), 0);
    t.extend_or_truncate_to(2).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.get_cell(Fk(1)).unwrap(), [0u8; 8]);
    assert_eq!(t.get_cell(Fk(2)).unwrap(), [0u8; 8]);
    t.truncate_to_count(1).unwrap();
    assert_eq!(t.count(), 1);
    assert!(t.get_cell(Fk(2)).is_err());
}

#[test]
fn uleb_typical_fits_in_cell() {
    let row = tiny(1000, 110, 112);
    let (cell, tail) = pack_cell(&row).unwrap();
    assert!(tail.is_none());
    assert_eq!(parse_cell(cell).unwrap(), CellParse::Complete(row));
    assert_eq!(row.size(), 222);
    assert_eq!(row.weight(), 4 * 110 + 112);
}

#[test]
fn former_n_in_byte_no_longer_forces_a_tail() {
    let row = tiny(50_000, 200, 200_000);
    let (cell, tail) = pack_cell(&row).unwrap();
    assert!(tail.is_none(), "three fields of this row fit in 8 B");
    assert_eq!(parse_cell(cell).unwrap(), CellParse::Complete(row));
}

#[test]
fn overflow_is_remaining_bytes_only() {
    let row = tiny(u64::from(u32::MAX), 4_000_000, 4_000_000);
    let stream = encode_stream(&row).unwrap();
    assert!(stream.len() > 8, "wide fee and sizes must miss 8 B");
    let (cell, tail) = pack_cell(&row).unwrap();
    let tail = tail.expect("tail");
    assert_eq!(tail.as_slice(), &stream[8..]);
    assert_eq!(parse_cell(cell).unwrap(), CellParse::NeedTail);
    assert_eq!(parse_with_tail(cell, &tail).unwrap(), row);
}

#[test]
fn header_blob_roundtrip() {
    let dir = TempDir::labeled("txstat-ovf").unwrap();
    let t = TxStat::create(&dir).unwrap();
    let small = tiny(1000, 110, 0);
    let fat = tiny(u64::from(u32::MAX), 4_000_000, 4_000_000);
    t.append_batch(0, &[small, fat]).unwrap();
    t.write_block_rows(Fk(1), 1, &[small, fat]).unwrap();
    let blob = t.header_blob(Fk(1)).unwrap();
    assert!(!blob.is_empty());
    let got = t.get_range(1, 2, Some(&blob)).unwrap();
    assert_eq!(got[0], Some(small));
    assert_eq!(got[1], Some(fat));
    assert_eq!(t.get_row_merged(Fk(2), 1, &blob).unwrap(), Some(fat));
}

#[test]
fn leftover_txfixed_body_unlinked_on_open() {
    let dir = TempDir::labeled("txstat-leftover").unwrap();
    std::fs::write(dir.join("txfixed.body"), b"junk").unwrap();
    let _t = TxStat::open(&dir).unwrap();
    assert!(!dir.join("txfixed.body").exists());
    assert!(dir.join("txstat.body").is_file());
}

#[test]
fn range_reads_the_cell_and_header_blob_reads_the_slot() {
    let dir = TempDir::labeled("txstat-slot").unwrap();
    let t = TxStat::create(&dir).unwrap();
    let small = tiny(1000, 110, 0);
    let fat = tiny(u64::from(u32::MAX), 4_000_000, 4_000_000);
    t.append_batch(0, &[small, fat]).unwrap();
    assert_eq!(t.get_range(1, 1, None).unwrap(), vec![Some(small)]);
    let err = match t.get_range(1, 4, None) {
        Ok(_) => panic!("range accepted a fk past the count"),
        Err(err) => err,
    };
    assert!(matches!(err, StoreError::NotFound), "{err:?}");

    assert!(t.header_blob(Fk(1)).unwrap().is_empty());
    t.write_block_rows(Fk(1), 1, &[small, fat]).unwrap();
    let blob = t.header_blob(Fk(1)).unwrap();
    assert!(!blob.is_empty());
    let got = t.get_range(1, 2, Some(&blob)).unwrap();
    assert_eq!(got[0], Some(small));
    assert_eq!(got[1], Some(fat));
    assert!(t.header_blob(Fk(2)).unwrap().is_empty());
}

/// Readers stamp unstamped blocks (Esplora block routes, `getblock`), so
/// `put_header_blob` runs on several threads at once. Each header must read
/// back its own blob. Equal-length blobs make a shared offset silent: the
/// slot still decodes, with another block's tails.
#[test]
fn concurrent_header_blobs_keep_their_own_bytes() {
    const THREADS: u64 = 8;
    const PER: u64 = 400;
    let dir = TempDir::labeled("txstat-ovf-race").unwrap();
    let t = std::sync::Arc::new(TxStat::create(&dir).unwrap());
    let blob_for = |fk: u64| {
        let tail = fk.to_le_bytes().repeat(4);
        encode_ovf_blob(&[(1, tail)]).unwrap()
    };
    let start = std::sync::Arc::new(std::sync::Barrier::new(THREADS as usize));
    let handles: Vec<_> = (0..THREADS)
        .map(|i| {
            let t = std::sync::Arc::clone(&t);
            let start = std::sync::Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                for k in 0..PER {
                    let fk = 1 + k * THREADS + i;
                    t.put_header_blob(Fk(fk), &blob_for(fk)).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let total: u64 = (1..=THREADS * PER)
        .map(|fk| blob_for(fk).len() as u64)
        .sum();
    assert_eq!(
        t.ovf.data_len(),
        total,
        "appends neither overlap nor leave gaps"
    );
    let wrong: Vec<u64> = (1..=THREADS * PER)
        .filter(|&fk| t.header_blob(Fk(fk)).unwrap() != blob_for(fk))
        .collect();
    assert!(wrong.is_empty(), "headers read another blob: {wrong:?}");
}

#[test]
fn open_refuses_a_legacy_txstat_ahead_journal() {
    let dir = TempDir::labeled("txstat-legacy-ahead").unwrap();
    let t = TxStat::create(&dir).unwrap();
    t.append_batch(0, &[tiny(1, 2, 3)]).unwrap();
    drop(t);
    std::fs::write(dir.join("txstat.body.ahead"), b"TTA1").unwrap();
    let err = match TxStat::open(&dir) {
        Ok(_) => panic!("open accepted a legacy ahead journal"),
        Err(err) => err,
    };
    match err {
        StoreError::Corrupt(msg) => assert!(msg.contains("txstat.body.ahead"), "{msg}"),
        other => panic!("{other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir.path());
}

/// A journal entry open cannot stat is not proof it is absent. A dangling
/// symlink at the journal name is refused like a journal.
#[cfg(unix)]
#[test]
fn open_refuses_a_dangling_txstat_ahead_entry() {
    let dir = TempDir::labeled("txstat-legacy-ahead-link").unwrap();
    let t = TxStat::create(&dir).unwrap();
    t.append_batch(0, &[tiny(1, 2, 3)]).unwrap();
    drop(t);
    std::os::unix::fs::symlink(dir.join("gone"), dir.join("txstat.body.ahead")).unwrap();
    match TxStat::open(&dir) {
        Ok(_) => panic!("open accepted a dangling ahead journal link"),
        Err(StoreError::Corrupt(msg)) => assert!(msg.contains("txstat.body.ahead"), "{msg}"),
        Err(other) => panic!("{other:?}"),
    }
    let _ = std::fs::remove_dir_all(dir.path());
}

/// The IBD archive syncs `txstat.ovf` once for the batch before any slot,
/// and leaves the slots to the Class A barrier. It does not sync per header.
#[cfg(debug_assertions)]
#[test]
fn ibd_overflows_sync_the_overflow_once_per_batch() {
    let dir = TempDir::labeled("txstat-ovf-batch").unwrap();
    let t = TxStat::create(&dir).unwrap();
    t.flush().unwrap();
    let ranges = [(Fk(1), Fk(1), 1), (Fk(2), Fk(2), 1), (Fk(3), Fk(3), 1)];
    let tails: Vec<(u64, Vec<u8>)> = (1..=3u64).map(|fk| (fk, vec![fk as u8; 4])).collect();
    let (ovf, blk) = (t.ovf.io_handle(), t.blk.io_handle());
    rbitcoin_store::io_handle::arm_fail_sync_after(ovf, 1, 1);
    rbitcoin_store::io_handle::arm_fail_sync(blk, 1);
    let put = t.put_overflows_for_headers(&ranges, &tails);
    rbitcoin_store::io_handle::disarm_pwrite(ovf);
    rbitcoin_store::io_handle::disarm_pwrite(blk);
    put.expect("one overflow sync and no slot sync for the batch");
    for fk in 1..=3u64 {
        let want = encode_ovf_blob(&[(0, vec![fk as u8; 4])]).unwrap();
        assert_eq!(t.header_blob(Fk(fk)).unwrap(), want);
    }
    let _ = std::fs::remove_dir_all(dir.path());
}

#[test]
fn append_batch_relies_on_the_class_a_tail_guard() {
    let dir = TempDir::labeled("txstat-append-no-ahead").unwrap();
    let t = TxStat::create(&dir).unwrap();
    t.append_batch(0, &[tiny(1, 2, 3)]).unwrap();
    assert_eq!(t.count(), 1);
    // The trace sees an arm that is cleared before the append returns.
    #[cfg(debug_assertions)]
    rbitcoin_store::page_guard::arm_store_mark_trace();
    let rows: Vec<TxStatRow> = (2..=1200u64).map(|i| tiny(i, 100, 10)).collect();
    t.append_batch(1, &rows).unwrap();
    #[cfg(debug_assertions)]
    assert!(
        rbitcoin_store::page_guard::take_armed_store_marks().is_empty(),
        "an append armed a mark"
    );
    assert!(!dir.join("txstat.body.ahead").exists());
    let _ = std::fs::remove_dir_all(dir.path());
}

/// The slot sync in `put_header_blob` must not publish a slot whose blob
/// bytes and `txstat.ovf` length are still unsynced. A failed overflow sync
/// stops the put before the slot is stamped.
#[cfg(debug_assertions)]
#[test]
fn header_blob_syncs_the_overflow_before_its_slot() {
    let dir = TempDir::labeled("txstat-ovf-order").unwrap();
    let t = TxStat::create(&dir).unwrap();
    t.flush().unwrap();
    let blob = encode_ovf_blob(&[(1, vec![0x5Au8; 32])]).unwrap();
    rbitcoin_store::io_handle::arm_fail_sync(t.ovf.io_handle(), 1);
    assert!(
        t.put_header_blob(Fk(1), &blob).is_err(),
        "the slot was stamped without syncing txstat.ovf"
    );
    assert!(t.header_blob(Fk(1)).unwrap().is_empty());
    t.put_header_blob(Fk(1), &blob).unwrap();
    assert_eq!(t.header_blob(Fk(1)).unwrap(), blob);
}

/// A stamp writes 8-byte cells, each inside one sector, so a tear leaves
/// each cell old or new. It arms no mark, and it is synced before it
/// returns: a failed sync fails the stamp.
#[cfg(debug_assertions)]
#[test]
fn an_interior_stamp_arms_no_mark_and_is_synced() {
    let dir = TempDir::labeled("txstat-interior-stamp").unwrap();
    let rows: Vec<TxStatRow> = (1..=1200u64).map(|i| tiny(i, 100, 10)).collect();
    let t = TxStat::create(&dir).unwrap();
    t.append_batch(0, &rows).unwrap();
    rbitcoin_store::page_guard::arm_store_mark_trace();
    t.write_row(Fk(600), &tiny(9, 9, 9)).unwrap();
    assert!(
        rbitcoin_store::page_guard::take_armed_store_marks().is_empty(),
        "an interior stamp armed a mark"
    );
    assert_eq!(
        t.get_cell(Fk(600)).unwrap(),
        pack_cell(&tiny(9, 9, 9)).unwrap().0
    );
    rbitcoin_store::io_handle::arm_fail_sync(t.body.io_handle(), 1);
    assert!(t.write_row(Fk(700), &tiny(8, 8, 8)).is_err());
    let _ = std::fs::remove_dir_all(dir.path());
}
