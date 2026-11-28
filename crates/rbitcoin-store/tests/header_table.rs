use rbitcoin_primitives::{Fk, TableKind};
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::{TableFile, FILE_HEADER_LEN};
use rbitcoin_store::hashhead::{initial_slots_for, HashHead, HeadScale};
use rbitcoin_store::header_table::*;

fn tmp() -> std::path::PathBuf {
    let p = rbitcoin_store::testutil::fresh_path("header");
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn sample(hash: [u8; 32]) -> HeaderRecord {
    HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 100,
        bits: 0x1d00ffff,
        nonce: 7,
        merkle_root: [2u8; 32],
        hash,
        size: 0,
        weight: 0,
    }
}

/// A lookup probes every generation of `header.head`, one file each. Each
/// roll doubles the slots of the generation before it, so the tiny scale
/// still rolls at 56 headers, and a chain of `n` headers costs a lookup
/// about log2(n) generations instead of n / 56.
#[test]
fn each_header_head_roll_doubles_the_generation() {
    let dir = tmp();
    let head = HeaderHead::create(dir.join("header.head"), HeadScale::Tiny).unwrap();
    let entries: Vec<([u8; 32], Fk)> = (1..=3_000u64)
        .map(|i| {
            let mut h = [0u8; 32];
            h[..8].copy_from_slice(&i.to_le_bytes());
            h[8] = 0x5a;
            (h, Fk(i))
        })
        .collect();
    head.insert_many(&entries).unwrap();
    let slots: Vec<u64> = head
        .gens
        .read()
        .unwrap()
        .iter()
        .map(|g| g.slots())
        .collect();
    assert_eq!(slots, vec![64, 128, 256, 512, 1_024, 2_048]);
    for (h, fk) in [entries[0], entries[55], entries[56], entries[2_999]] {
        assert_eq!(head.get_all(&h).unwrap(), vec![fk]);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn header_put_get_by_hash_open_flush() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let h1 = [1u8; 32];
    let h2 = [2u8; 32];
    let fk1 = t.ensure(&sample(h1)).unwrap();
    let fk2 = t.ensure(&sample(h2)).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.get(fk1).unwrap().hash, h1);
    assert_eq!(t.get(fk2).unwrap().hash, h2);
    assert_eq!(t.get_by_hash(&h1).unwrap().unwrap().0, fk1);
    assert!(t.get_by_hash(&[9u8; 32]).unwrap().is_none());
    assert!(matches!(t.get(Fk::NULL), Err(StoreError::InvalidFk)));
    assert!(matches!(t.get(Fk(99)), Err(StoreError::NotFound)));
    assert!(matches!(
        HeaderRecord::decode(&[0u8; 10]),
        Err(StoreError::Corrupt(_))
    ));
    t.flush().unwrap();
    t.flush_async().unwrap();
    drop(t);
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.get_by_hash(&h2).unwrap().unwrap().1.nonce, 7);
    // Shrink OS file below HWM so open clamps logical to a non-record size.
    {
        use rbitcoin_store::file::FILE_HEADER_LEN;
        let body = dir.join("header.body");
        std::fs::OpenOptions::new()
            .write(true)
            .open(&body)
            .unwrap()
            .set_len((FILE_HEADER_LEN + 3) as u64)
            .unwrap();
    }
    assert!(matches!(
        HeaderTable::open_tiny(&dir),
        Err(StoreError::Corrupt(_))
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_flush_holds_put_lock_until_its_flushes_finish() {
    use rbitcoin_store::array_table::TipFlushHold;
    use rbitcoin_store::bind_header_flush_hold;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::Duration;

    let dir = tmp();
    let t = Arc::new(HeaderTable::create_tiny(&dir).unwrap());
    t.ensure(&sample([1u8; 32])).unwrap();
    let hold = Arc::new(TipFlushHold::new(true));
    let flush_t = Arc::clone(&t);
    let flush_hold = Arc::clone(&hold);
    let handle = std::thread::spawn(move || {
        bind_header_flush_hold(Some(Arc::clone(&flush_hold)));
        flush_t.flush().unwrap();
        bind_header_flush_hold(None);
    });
    assert!(
        hold.wait_until_entered(Duration::from_secs(5)),
        "flush never reached the hold"
    );
    assert!(
        matches!(
            t.put_lock.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ),
        "an insert could land in the dirty pages before the mark was cleared"
    );
    hold.store(false, Ordering::Release);
    handle.join().unwrap();
    t.ensure(&sample([2u8; 32])).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Real block-header fields with PoW-style hash committed to a real parent.
fn linked_child(parent: &HeaderRecord, parent_fk: Fk, salt: u32) -> HeaderRecord {
    let version = 1;
    let timestamp = 1_700_000_000 + salt;
    let bits = 0x207fffff;
    let nonce = salt;
    let mut merkle = [0u8; 32];
    merkle[0..4].copy_from_slice(&salt.to_le_bytes());
    let hash = block_header_hash(version, &parent.hash, &merkle, timestamp, bits, nonce);
    HeaderRecord {
        prev_fk: parent_fk,
        version,
        timestamp,
        bits,
        nonce,
        merkle_root: merkle,
        hash,
        size: 0,
        weight: 0,
    }
}

#[test]
fn ensure_batch_linked_headers_roundtrip() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let g = sample([0x10; 32]);
    let a = linked_child(&g, Fk(1), 1);
    let b = linked_child(&a, Fk(2), 2);
    let fks = t.ensure_batch(&[g.clone(), a.clone(), b.clone()]).unwrap();
    assert_eq!(fks, vec![Fk(1), Fk(2), Fk(3)]);
    assert_eq!(t.count(), 3);
    assert_eq!(t.get_by_hash(&g.hash).unwrap().unwrap().0, Fk(1));
    assert_eq!(t.get_by_hash(&b.hash).unwrap().unwrap().0, Fk(3));
    drop(t);
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 3);
    assert_eq!(t.get_by_hash(&a.hash).unwrap().unwrap().1.nonce, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The first append's data sync fails before any length moves. It leaves
/// `guard/header.body` as an empty flag, not page images, and no row. Open
/// finds no row, drops the flag, and takes that header again at fk 1.
#[test]
fn a_failed_first_append_leaves_an_empty_flag_and_no_row() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    // The header sync before the flag passes; the row data sync fails.
    rbitcoin_store::io_handle::arm_fail_sync_after(t.body.io_handle(), 1, 1);
    let g = sample([0x91; 32]);
    let err = t.ensure_batch(std::slice::from_ref(&g)).unwrap_err();
    assert!(matches!(err, StoreError::Io { .. }), "{err}");
    let flag = dir.join(rbitcoin_store::page_guard::HEADER_BODY_MARK);
    assert_eq!(
        std::fs::metadata(&flag).expect("no flag").len(),
        0,
        "the first append's flag holds page images"
    );
    drop(t);
    let t = HeaderTable::open_tiny(&dir).expect("open after a failed body sync");
    assert_eq!(t.count(), 0);
    assert_eq!(t.get_by_hash(&g.hash).unwrap(), None);
    assert!(!flag.exists(), "open left the flag");
    assert_eq!(t.ensure(&g).unwrap(), Fk(1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Store creation leaves the body header unsynced. The first append syncs
/// it before the flag, so open never meets a flag over a header that is not
/// on the device.
#[cfg(all(unix, debug_assertions))]
#[test]
fn the_first_header_append_syncs_the_body_before_its_flag() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    rbitcoin_store::file::arm_fsync_dir_trace();
    t.ensure(&sample([0x97; 32])).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    let body_at = synced.iter().position(|p| p == &dir.join("header.body"));
    let guard_at = synced.iter().position(|p| p == &dir.join("guard"));
    assert!(
        matches!((body_at, guard_at), (Some(b), Some(g)) if b < g),
        "header.body was not synced before the flag: {synced:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Only the first header append arms `guard/header.body`. A later append
/// writes its rows past the end, syncs, then publishes the length, so it
/// arms nothing.
#[cfg(debug_assertions)]
#[test]
fn a_header_append_after_the_first_arms_no_mark() {
    use rbitcoin_store::page_guard::{arm_store_mark_trace, take_armed_store_marks};
    let mark = rbitcoin_store::page_guard::HEADER_BODY_MARK.to_string();
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    arm_store_mark_trace();
    t.ensure(&sample([0x92; 32])).unwrap();
    let first = take_armed_store_marks();
    assert!(
        first.contains(&mark),
        "the first append armed no flag: {first:?}"
    );
    arm_store_mark_trace();
    t.ensure(&sample([0x93; 32])).unwrap();
    let later = take_armed_store_marks();
    assert!(!later.contains(&mark), "a later append armed {later:?}");
    assert!(!dir.join(&mark).exists());
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed length flush may leave the new body length durable. The table
/// refuses later header puts until reopen with its own error, so no retry
/// rewrites rows below that length. Reopen indexes the row the length names.
#[cfg(debug_assertions)]
#[test]
fn a_failed_length_flush_refuses_header_puts_until_reopen() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    t.ensure(&sample([0x94; 32])).unwrap();
    rbitcoin_store::io_handle::arm_fail_flush(t.body.io_handle(), 1);
    let g = sample([0x95; 32]);
    assert!(
        t.ensure(&g).is_err(),
        "the failed length flush was not reported"
    );
    let next = sample([0x96; 32]);
    match t.ensure(&next) {
        Err(StoreError::Rejected(m)) => assert!(m.contains("reopen"), "{m}"),
        other => panic!("a put after a failed length flush: {other:?}"),
    }
    drop(t);
    let t = HeaderTable::open_tiny(&dir).expect("open after a failed length flush");
    // The length write reached the page cache before its flush failed.
    assert_eq!(t.count(), 2);
    assert_eq!(
        t.get_by_hash(&g.hash).unwrap().map(|(fk, _)| fk),
        Some(Fk(2))
    );
    assert_eq!(t.ensure(&next).unwrap(), Fk(3));
    let _ = std::fs::remove_dir_all(&dir);
}

/// A crash inside a head insert can leave the head naming any subset of
/// the last batch, not a suffix: a generation flushes its slot-ordered rows
/// and then rolls. Open checks every row of the last chunk, so a lower row
/// the head does not name is indexed even when a higher one is.
#[test]
fn open_indexes_an_unnamed_row_below_a_named_one() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    t.ensure(&sample([0xA1; 32])).unwrap();
    let (b, c) = (sample([0xA2; 32]), sample([0xA3; 32]));
    let at = t.body.logical_len();
    let rows = [b.encode(), c.encode()].concat();
    rbitcoin_store::page_guard::commit_appends(&[rbitcoin_store::page_guard::Append {
        file: &t.body,
        at,
        bytes: &rows,
        new_logical: at + rows.len() as u64,
        length_wave: 0,
    }])
    .unwrap();
    t.head.insert_many(&[(c.hash, Fk(3))]).unwrap();
    drop(t);
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 3);
    assert_eq!(
        t.get_by_hash(&b.hash).unwrap().map(|(fk, _)| fk),
        Some(Fk(2)),
        "the unnamed row below a named one was not indexed"
    );
    assert_eq!(t.ensure(&b).unwrap(), Fk(2), "a second row for one hash");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A batch past one chunk commits as several appends, each named in the
/// head before the next starts. A failure in the third chunk leaves the
/// first two durable and named. Row 1 sits below open's last-chunk scan,
/// so only the first chunk's own head insert can have named it.
#[cfg(debug_assertions)]
#[test]
fn a_failure_past_the_first_chunk_keeps_that_chunk_named() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let recs: Vec<HeaderRecord> = (0..4_001u32)
        .map(|i| {
            let mut h = [0x5bu8; 32];
            h[..4].copy_from_slice(&i.to_le_bytes());
            sample(h)
        })
        .collect();
    // The header sync before the flag and the first two chunks' data syncs
    // pass; the third chunk's data sync fails.
    rbitcoin_store::io_handle::arm_fail_sync_after(t.body.io_handle(), 3, 1);
    assert!(
        t.ensure_batch(&recs).is_err(),
        "the third chunk did not fail"
    );
    drop(t);
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 4_000);
    for i in [0usize, 1_999, 3_999] {
        let found = t.get_by_hash(&recs[i].hash).unwrap().map(|(fk, _)| fk);
        assert_eq!(found, Some(Fk(i as u64 + 1)), "row {i}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A failed head insert leaves durable rows the head does not name. A retry
/// of the same header must not append a second row: the table refuses puts
/// until reopen, which indexes the row.
#[test]
fn a_failed_head_insert_refuses_header_puts_until_reopen() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    t.ensure(&sample([0xB1; 32])).unwrap();
    // A directory where the head mark goes: the head insert cannot arm.
    let mark = dir.join(rbitcoin_store::page_guard::HEADER_HEAD_MARK);
    std::fs::create_dir_all(mark.join("block")).unwrap();
    let g = sample([0xB2; 32]);
    assert!(t.ensure(&g).is_err(), "the head insert did not fail");
    match t.ensure(&g) {
        Err(StoreError::Rejected(m)) => assert!(m.contains("reopen"), "{m}"),
        other => panic!("a retry after a failed head insert: {other:?}"),
    }
    drop(t);
    std::fs::remove_dir_all(&mark).unwrap();
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 2);
    assert_eq!(t.ensure(&g).unwrap(), Fk(2), "a second row for one hash");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_batch_duplicate_hash_is_one_row() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let g = sample([0x21; 32]);
    let fks = t.ensure_batch(&[g.clone(), g.clone()]).unwrap();
    assert_eq!(fks[0], fks[1]);
    assert_eq!(t.count(), 1);
    let again = t.ensure_batch(&[g]).unwrap();
    assert_eq!(again[0], fks[0]);
    assert_eq!(t.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_batch_false_parent_writes_nothing() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let g = sample([0x31; 32]);
    let a = linked_child(&g, Fk(1), 1);
    let honest = linked_child(&a, Fk(2), 2);
    let mut lying = honest.clone();
    lying.prev_fk = Fk(1);
    let err = t.ensure_batch(&[g.clone(), a.clone(), lying]).unwrap_err();
    assert!(
        matches!(err, StoreError::Corrupt(_)),
        "false parent edge must be rejected, got {err}"
    );
    assert_eq!(t.count(), 0, "failed batch must not append");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_batch_parent_in_batch_assigns_prev_fk() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let g = sample([0x41; 32]);
    let a = linked_child(&g, Fk(1), 7);
    let fks = t.ensure_batch(&[g, a.clone()]).unwrap();
    assert_eq!(fks, vec![Fk(1), Fk(2)]);
    assert_eq!(t.get(Fk(2)).unwrap().prev_fk, Fk(1));
    assert_eq!(t.get(Fk(2)).unwrap().hash, a.hash);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Production failure shape: same full hash as an older block, but `prev_fk`
/// points at a tip-extension header. Without the write gate this plants a
/// false child edge that resume walks as "headers past tip".
#[test]
fn ensure_rejects_duplicate_hash_with_divergent_prev_and_false_parent_edge() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();

    // G (null prev, synthetic hash) → A → B → C (real linked hashes).
    let g = sample([0x11; 32]);
    let g_fk = t.ensure(&g).unwrap();
    let a = linked_child(&g, g_fk, 1);
    let a_fk = t.ensure(&a).unwrap();
    let b = linked_child(&a, a_fk, 2);
    let b_fk = t.ensure(&b).unwrap();
    let c = linked_child(&b, b_fk, 3);
    let c_fk = t.ensure(&c).unwrap();
    assert_eq!(t.count(), 4);

    // Poison: re-insert G's identity with prev_fk = C (false parent).
    let mut poison = g.clone();
    poison.prev_fk = c_fk;
    let again = t.ensure(&poison).unwrap();
    assert_eq!(again, g_fk, "same hash must not create a second row");
    assert_eq!(t.count(), 4, "duplicate hash must not grow the table");
    assert_eq!(t.get(g_fk).unwrap().prev_fk, Fk::NULL);

    // First-time insert of a header whose hash commits to A as parent, but
    // caller lies with prev_fk = C → corrupt (false parent edge).
    let honest = linked_child(&a, a_fk, 99);
    let mut lying = honest.clone();
    lying.prev_fk = c_fk;
    let err = t.ensure(&lying).unwrap_err();
    assert!(
        matches!(err, StoreError::Corrupt(_)),
        "false parent edge must be rejected at write gate, got {err}"
    );
    assert_eq!(t.count(), 4);

    let ok = t.ensure(&honest).unwrap();
    assert_eq!(t.count(), 5);
    assert_eq!(t.get(ok).unwrap().prev_fk, a_fk);

    // Children of C: only what truly points at C (none of the poisons).
    let mut kids_of_c = 0u32;
    for id in 1..=t.count() {
        let rec = t.get(Fk(id)).unwrap();
        if rec.prev_fk == c_fk {
            kids_of_c += 1;
        }
    }
    assert_eq!(
        kids_of_c, 0,
        "C must not gain false children from poison puts"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn body_scan_stops_at_the_callers_count_and_bits_match_rows() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let bits = [0x207f_ffff, 0x1e0f_ffff, 0x1d00_ffff];
    for (i, b) in bits.into_iter().enumerate() {
        let mut rec = sample([i as u8 + 1; 32]);
        rec.bits = b;
        t.ensure(&rec).unwrap();
    }
    let mut seen = Vec::new();
    t.for_each_record_through(2, |fk, rec| {
        seen.push((fk.0, rec.bits));
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, vec![(1, bits[0]), (2, bits[1])]);
    let got = t.bits_in_fk_order().unwrap();
    assert_eq!(got, bits);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_head_rolls_generation_when_gen0_is_full() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let mut hashes = Vec::new();
    for i in 0u32..80 {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&i.to_le_bytes());
        hash[4] = 0xa5;
        hashes.push(hash);
        t.ensure(&sample(hash)).unwrap();
    }
    assert!(
        dir.join("header.head.g1").is_file(),
        "tiny 64-slot gen0 must roll header.head.g1"
    );
    assert!(dir.join("header.head").is_file());
    let first = t.get_by_hash(&hashes[0]).unwrap().unwrap();
    let last = t.get_by_hash(&hashes[79]).unwrap().unwrap();
    assert_eq!(first.1.hash, hashes[0]);
    assert_eq!(last.1.hash, hashes[79]);
    assert_eq!(t.ensure(&sample(hashes[0])).unwrap(), first.0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_batch_rolls_generation_when_gen0_is_full() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let recs: Vec<HeaderRecord> = (0u32..80)
        .map(|i| {
            let mut hash = [0u8; 32];
            hash[0..4].copy_from_slice(&i.to_le_bytes());
            hash[4] = 0xa5;
            sample(hash)
        })
        .collect();
    let fks = t.ensure_batch(&recs).unwrap();
    assert_eq!(fks.len(), 80);
    assert_eq!(t.count(), 80);
    assert!(
        dir.join("header.head.g1").is_file(),
        "tiny 64-slot gen0 must roll header.head.g1"
    );
    assert_eq!(t.get_by_hash(&recs[0].hash).unwrap().unwrap().0, Fk(1));
    assert_eq!(t.get_by_hash(&recs[79].hash).unwrap().unwrap().0, Fk(80));
    drop(t);
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(
        t.get_by_hash(&recs[40].hash).unwrap().unwrap().1.hash,
        recs[40].hash
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_head_open_grows_undersized_single_gen() {
    let dir = tmp();
    let hashes: Vec<[u8; 32]> = (0u32..5)
        .map(|i| {
            let mut h = [0u8; 32];
            h[0..4].copy_from_slice(&i.to_le_bytes());
            h[8] = 0x3c;
            h
        })
        .collect();
    {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header).unwrap();
        for (i, hash) in hashes.iter().enumerate() {
            let rec = sample(*hash);
            let off = FILE_HEADER_LEN as u64 + (i as u64) * HEADER_RECORD_LEN as u64;
            body.write_at(off, &rec.encode()).unwrap();
        }
        body.set_logical_len(FILE_HEADER_LEN as u64 + 5 * HEADER_RECORD_LEN as u64)
            .unwrap();
        body.flush().unwrap();
        let h = HashHead::create_with_slots(dir.join("header.head"), 32).unwrap();
        for (i, hash) in hashes.iter().enumerate() {
            h.insert(hash, Fk(i as u64 + 1)).unwrap();
        }
        h.flush().unwrap();
        assert_eq!(h.slots(), 32);
    }
    #[cfg(unix)]
    let old = std::fs::File::open(dir.join("header.head")).unwrap();
    #[cfg(unix)]
    let old_ino = {
        use std::os::unix::fs::MetadataExt;
        old.metadata().unwrap().ino()
    };
    let t = HeaderTable::open_tiny(&dir).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            std::fs::metadata(dir.join("header.head")).unwrap().ino(),
            old_ino,
            "HeaderTable::open must replace undersized header.head, not punch it"
        );
        drop(old);
    }
    for hash in &hashes {
        assert_eq!(t.get_by_hash(hash).unwrap().unwrap().1.hash, *hash);
    }
    for i in 5u32..40 {
        let mut hash = [0u8; 32];
        hash[0..4].copy_from_slice(&i.to_le_bytes());
        hash[8] = 0x3c;
        t.ensure(&sample(hash)).unwrap();
    }
    assert!(
        !dir.join("header.head.g1").is_file(),
        "open-grow to 64 slots must absorb 40 headers without rolling"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_head_empty_target_sized_gen0_with_body_is_layout_refuse() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    t.ensure(&sample([0x44; 32])).unwrap();
    t.flush().unwrap();
    drop(t);
    std::fs::remove_file(dir.join("header.head")).unwrap();
    let slots = initial_slots_for(HeadScale::Tiny);
    let staging = tmp();
    let h = HashHead::create_with_slots(staging.join("header.head"), slots).unwrap();
    h.flush().unwrap();
    drop(h);
    std::fs::rename(staging.join("header.head"), dir.join("header.head")).unwrap();
    let _ = std::fs::remove_dir_all(&staging);
    let err = match HeaderTable::open_tiny(&dir) {
        Err(e) => e,
        Ok(_) => panic!("expected Layout refuse for empty target-sized header.head"),
    };
    match err {
        StoreError::Layout(m) => {
            assert!(m.contains("header.head"), "{m}");
            assert!(m.contains("header.body"), "{m}");
            assert!(m.contains("header.head.mlt"), "{m}");
        }
        other => panic!("expected Layout, got {other}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_head_directory_is_layout_refuse() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    drop(t);
    let head = dir.join("header.head");
    std::fs::remove_file(&head).unwrap();
    std::fs::create_dir(&head).unwrap();
    std::fs::write(head.join("00"), b"x").unwrap();
    std::fs::write(head.join("01"), b"y").unwrap();
    let err = match HeaderTable::open_tiny(&dir) {
        Err(e) => e,
        Ok(_) => panic!("expected Layout refuse for sharded header.head dir"),
    };
    match err {
        StoreError::Layout(m) => {
            assert!(m.contains("header.head"), "{m}");
        }
        other => panic!("expected Layout, got {other}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_same_hash_twice_is_idempotent() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    let h = sample([7u8; 32]);
    let fk1 = t.ensure(&h).unwrap();
    let mut h2 = h.clone();
    h2.prev_fk = Fk(999); // divergent prev ignored on hit
    let fk2 = t.ensure(&h2).unwrap();
    assert_eq!(fk1, fk2);
    assert_eq!(t.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_record_roundtrip_is_88_bytes() {
    let rec = HeaderRecord {
        prev_fk: Fk(0x0807_0605_0403_0201),
        version: 0x0403_0201,
        timestamp: 0x0807_0605,
        bits: 0x0c0b_0a09,
        nonce: 0x100f_0e0d,
        merkle_root: std::array::from_fn(|i| i as u8),
        hash: std::array::from_fn(|i| (i as u8).wrapping_add(32)),
        size: 285,
        weight: 1140,
    };
    let enc = rec.encode();
    assert_eq!(enc.len(), HEADER_RECORD_LEN);
    let back = HeaderRecord::decode(&enc).unwrap();
    assert_eq!(back.size, 0);
    assert_eq!(back.weight, 0);
    assert_eq!(back.prev_fk, rec.prev_fk);
    assert_eq!(back.version, rec.version);
    assert_eq!(back.timestamp, rec.timestamp);
    assert_eq!(back.bits, rec.bits);
    assert_eq!(back.nonce, rec.nonce);
    assert_eq!(back.merkle_root, rec.merkle_root);
    assert_eq!(back.hash, rec.hash);
    assert!(matches!(
        HeaderRecord::decode(&[0u8; 40]),
        Err(StoreError::Corrupt(m)) if m == "short header record"
    ));
    let mut longer = [0u8; HEADER_RECORD_LEN_V24];
    longer[..HEADER_RECORD_LEN].copy_from_slice(&enc);
    let wide = HeaderRecord::decode(&longer).unwrap();
    assert_eq!(wide.hash, rec.hash);
    assert_eq!(wide.nonce, rec.nonce);
}

#[test]
fn rewrite_v24_header_body_strips_size_weight() {
    let dir = tmp();
    let rec = sample([0x11; 32]);
    {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header).unwrap();
        let mut raw = [0u8; HEADER_RECORD_LEN_V24];
        raw[..HEADER_RECORD_LEN].copy_from_slice(&rec.encode());
        raw[88..92].copy_from_slice(&285u32.to_le_bytes());
        raw[92..96].copy_from_slice(&1140u32.to_le_bytes());
        body.write_at(FILE_HEADER_LEN as u64, &raw).unwrap();
        body.set_logical_len(FILE_HEADER_LEN as u64 + HEADER_RECORD_LEN_V24 as u64)
            .unwrap();
        body.flush().unwrap();
        let h = HashHead::create_with_slots(dir.join("header.head"), 64).unwrap();
        h.insert(&rec.hash, Fk(1)).unwrap();
        h.flush().unwrap();
    }
    HeaderTable::rewrite_v24_body_to_88(&dir).unwrap();
    let t = HeaderTable::open_tiny(&dir).unwrap();
    let got = t.get(Fk(1)).unwrap();
    assert_eq!(got.hash, rec.hash);
    assert_eq!(got.size, 0);
    assert_eq!(got.weight, 0);
    assert_eq!(t.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn header_head_occupied_sums_base_and_gen() {
    let dir = tmp();
    assert_eq!(header_head_occupied(&dir).unwrap(), 0);
    {
        let h = HashHead::create_with_slots(dir.join("header.head"), 64).unwrap();
        h.insert(&[0x11; 32], Fk(1)).unwrap();
        h.flush().unwrap();
    }
    assert_eq!(header_head_occupied(&dir).unwrap(), 1);
    {
        let g = HashHead::create_with_slots(dir.join("header.head.g1"), 64).unwrap();
        g.insert(&[0x22; 32], Fk(2)).unwrap();
        g.insert(&[0x33; 32], Fk(3)).unwrap();
        g.flush().unwrap();
    }
    assert_eq!(header_head_occupied(&dir).unwrap(), 3);
    std::fs::write(dir.join("header.head"), [0u8; 3]).unwrap();
    assert!(header_head_occupied(&dir).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rewrite_v24_already_88_is_nop() {
    let dir = tmp();
    let t = HeaderTable::create_tiny(&dir).unwrap();
    t.ensure(&sample([0x22; 32])).unwrap();
    t.flush().unwrap();
    drop(t);
    HeaderTable::rewrite_v24_body_to_88(&dir).unwrap();
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

/// 11×96 B is the same byte length as 12×88 B. Occupied 11 must strip.
#[test]
fn rewrite_v24_ambiguous_96_strips_each_row() {
    let dir = tmp();
    let n = 11u64;
    let recs: Vec<HeaderRecord> = (0..n).map(|i| sample([0xA0 + i as u8; 32])).collect();
    {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header).unwrap();
        let mut blob = Vec::with_capacity(n as usize * HEADER_RECORD_LEN_V24);
        for rec in &recs {
            let mut raw = [0u8; HEADER_RECORD_LEN_V24];
            raw[..HEADER_RECORD_LEN].copy_from_slice(&rec.encode());
            raw[88..92].copy_from_slice(&7u32.to_le_bytes());
            blob.extend_from_slice(&raw);
        }
        body.write_at(FILE_HEADER_LEN as u64, &blob).unwrap();
        body.set_logical_len(FILE_HEADER_LEN as u64 + blob.len() as u64)
            .unwrap();
        body.flush().unwrap();
        let h = HashHead::create_with_slots(dir.join("header.head"), 64).unwrap();
        for (i, rec) in recs.iter().enumerate() {
            h.insert(&rec.hash, Fk(i as u64 + 1)).unwrap();
        }
        h.flush().unwrap();
    }
    HeaderTable::rewrite_v24_body_to_88(&dir).unwrap();
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), n);
    for (i, rec) in recs.iter().enumerate() {
        let got = t.get(Fk(i as u64 + 1)).unwrap();
        assert_eq!(got.hash, rec.hash);
        assert_eq!(got.size, 0);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// 12×88 B is the same byte length as 11×96 B. Occupied 12 must not strip.
#[test]
fn rewrite_v24_ambiguous_88_stays() {
    let dir = tmp();
    let n = 12u64;
    let recs: Vec<HeaderRecord> = (0..n).map(|i| sample([0xB0 + i as u8; 32])).collect();
    {
        let body = TableFile::create(dir.join("header.body"), TableKind::Header).unwrap();
        let mut blob = Vec::with_capacity(n as usize * HEADER_RECORD_LEN);
        for rec in &recs {
            blob.extend_from_slice(&rec.encode());
        }
        body.write_at(FILE_HEADER_LEN as u64, &blob).unwrap();
        body.set_logical_len(FILE_HEADER_LEN as u64 + blob.len() as u64)
            .unwrap();
        body.flush().unwrap();
        let h = HashHead::create_with_slots(dir.join("header.head"), 64).unwrap();
        for (i, rec) in recs.iter().enumerate() {
            h.insert(&rec.hash, Fk(i as u64 + 1)).unwrap();
        }
        h.flush().unwrap();
    }
    let before = std::fs::metadata(dir.join("header.body")).unwrap().len();
    HeaderTable::rewrite_v24_body_to_88(&dir).unwrap();
    assert_eq!(
        std::fs::metadata(dir.join("header.body")).unwrap().len(),
        before
    );
    let t = HeaderTable::open_tiny(&dir).unwrap();
    assert_eq!(t.count(), n);
    assert_eq!(t.get(Fk(n)).unwrap().hash, recs[n as usize - 1].hash);
    let _ = std::fs::remove_dir_all(&dir);
}
