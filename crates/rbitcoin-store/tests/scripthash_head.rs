use rbitcoin_primitives::Fk;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::FILE_HEADER_LEN;
use rbitcoin_store::scripthash_head::*;
use rbitcoin_store::scripthash_layout::{ShHeadValue, SH_HEAD_SLOT_SIZE};
use std::time::Instant;

#[test]
fn prefix_shard_of_high_bits_contiguous() {
    // 16 shards: first nibble of byte 0.
    assert_eq!(prefix_shard_of(&[0x00; 32], 16), 0);
    assert_eq!(prefix_shard_of(&[0x0f; 32], 16), 0);
    let mut k = [0u8; 32];
    k[0] = 0x10;
    assert_eq!(prefix_shard_of(&k, 16), 1);
    k[0] = 0x1f;
    assert_eq!(prefix_shard_of(&k, 16), 1);
    k[0] = 0xf0;
    assert_eq!(prefix_shard_of(&k, 16), 15);
    k[0] = 0xff;
    assert_eq!(prefix_shard_of(&k, 16), 15);
    // Lex order of first byte maps to non-decreasing shard ids.
    let mut prev = 0usize;
    for b in 0u16..=255 {
        k[0] = b as u8;
        let s = prefix_shard_of(&k, 16);
        assert!(s >= prev, "b={b:#x} shard={s} prev={prev}");
        prev = s;
    }
    assert_eq!(prefix_shard_of(&[0xab; 32], 1), 0);
}

#[test]
fn head_insert_get_clear() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let h = ScriptHashHead::create_with_slots(&path, 64).unwrap();
    let mut key = [0u8; 32];
    key[0] = 7;
    let val = ShHeadValue::inline_one(Fk(42));
    h.insert(&key, &val).unwrap();
    assert_eq!(h.get(&key).unwrap().unwrap(), val);
    assert!(h.clear_key(&key).unwrap());
    assert!(h.get(&key).unwrap().is_none());
    // Sidecar tracks occupied; reopen without full scan path for tiny files.
    drop(h);
    let h2 = ScriptHashHead::open(&path).unwrap().expect("live head");
    assert!(h2.is_known_empty() || h2.occupied() >= 1); // soft-clear keeps slot
    assert!(h2.get(&key).unwrap().is_none());
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

#[test]
fn open_large_without_occ_skips_scan_not_empty() {
    // Body just over OCC_SCAN_BYTE_CAP → skip full scan when .occ missing.
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-large-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    // body = slots * 32 B; need body > 16 MiB ⇒ slots > 16MiB/32 = 524_288.
    let min_slots = (OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1;
    let slots = min_slots.next_power_of_two();
    assert!(slots * SH_HEAD_SLOT_SIZE as u64 > OCC_SCAN_BYTE_CAP);
    let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
    assert!(h.is_known_empty());
    let mut key = [0u8; 32];
    key[0] = 0xab;
    h.insert(&key, &ShHeadValue::inline_one(Fk(1))).unwrap();
    assert_eq!(h.occupied(), 1);
    drop(h);
    // Drop sidecar only — reopen must not scan body and must not claim empty.
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let t0 = Instant::now();
    let h2 = ScriptHashHead::open(&path).unwrap().expect("live head");
    let open_ms = t0.elapsed().as_millis();
    assert!(
        !h2.is_known_empty(),
        "missing .occ on large head must not report empty"
    );
    assert!(
        open_ms < 2_000,
        "open without .occ must skip full scan (took {open_ms}ms)"
    );
    // Lookups still work (probe, not occupancy).
    assert_eq!(h2.get(&key).unwrap().unwrap().inline_fks(), vec![Fk(1)]);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

/// A kill after the table write and before the sidecar rename leaves live
/// slots with `.occ` still 0. The next `insert_many` must not bulk-fill
/// over those slots.
#[test]
fn zero_occ_sidecar_does_not_bulk_fill_over_live_slots() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-zero-occ-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let min_slots = (OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1;
    let slots = min_slots.next_power_of_two();
    assert!(slots * SH_HEAD_SLOT_SIZE as u64 > OCC_SCAN_BYTE_CAP);
    let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
    let mut key_a = [0u8; 32];
    key_a[0] = 0x11;
    let val_a = ShHeadValue::inline_one(Fk(7));
    h.insert_many(&[(key_a, val_a.clone())]).unwrap();
    assert_eq!(h.get(&key_a).unwrap().unwrap(), val_a);
    drop(h);
    store_occ_sidecar(&path, 0).unwrap();
    let h = ScriptHashHead::open(&path).unwrap().expect("live head");
    assert!(
        !h.is_known_empty(),
        "a zero .occ on a live head reported empty"
    );
    assert_eq!(h.occupied(), 1, "open kept the zero .occ count");
    let mut key_b = [0u8; 32];
    key_b[0] = 0x22;
    let val_b = ShHeadValue::inline_one(Fk(9));
    h.insert_many(&[(key_b, val_b.clone())]).unwrap();
    assert_eq!(
        h.get(&key_a).unwrap().map(|v| v.inline_fks()),
        Some(vec![Fk(7)]),
        "a zero .occ let the next insert overwrite the live head"
    );
    assert_eq!(h.get(&key_b).unwrap().unwrap().inline_fks(), vec![Fk(9)]);
    assert_eq!(h.occupied(), 2, "open count plus the new key");
    assert_eq!(load_occ_sidecar(&path), Some(2));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

/// A large ingest this process created and never filled is still `.occ` 0.
/// Open must remove it so the next start has no table to scan.
#[test]
fn open_removes_a_large_empty_zero_occ() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-empty-occ-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let min_slots = (OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1;
    let slots = min_slots.next_power_of_two();
    assert!(slots * SH_HEAD_SLOT_SIZE as u64 > OCC_SCAN_BYTE_CAP);
    let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
    assert!(h.is_known_empty());
    drop(h);
    assert!(path.exists());
    let opened = ScriptHashHead::open(&path).unwrap();
    assert!(opened.is_none(), "open kept a large empty ingest");
    assert!(!path.exists(), "open left a large empty ingest on disk");
    assert!(
        !occ_sidecar_path(&path).exists(),
        "open left a zero occupancy sidecar on disk"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

/// `insert_many` must surface a `.occ` rename error. A directory at the
/// sidecar path makes that rename fail.
#[test]
fn insert_many_returns_sidecar_rename_error() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-occ-dir-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let occ = occ_sidecar_path(&path);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&occ);
    let h = ScriptHashHead::create_with_slots(&path, 8).unwrap();
    std::fs::remove_file(&occ).unwrap();
    std::fs::create_dir(&occ).unwrap();
    let mut key = [0u8; 32];
    key[0] = 1;
    let err = h.insert_many(&[(key, ShHeadValue::inline_one(Fk(1)))]);
    assert!(err.is_err(), "a sidecar rename error must fail insert_many");
    drop(h);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_dir_all(&occ);
}

#[test]
fn scripthash_head_insert_many_full_without_rehash() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-full-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let h = ScriptHashHead::create_with_slots(&path, 8).unwrap();
    h.insert_many(&[]).unwrap();
    let mut k0 = [0u8; 32];
    k0[0] = 1;
    h.insert(&k0, &ShHeadValue::Empty).unwrap();
    assert!(!h.clear_key(&k0).unwrap());

    let mut n = 0u64;
    loop {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&n.to_le_bytes());
        match h.insert(&key, &ShHeadValue::inline_one(Fk(n + 1))) {
            Ok(()) => n += 1,
            Err(StoreError::Corrupt(SH_HEAD_FULL)) => break,
            Err(e) => panic!("unexpected {e}"),
        }
        assert!(n < 16, "must not grow 8-slot ingest OA");
    }
    let mut seen = 0u64;
    h.for_each_occupied(|_full, val| {
        assert!(!val.is_empty());
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert!(seen >= 1);
    h.flush().unwrap();
    h.flush_async().unwrap();
    drop(h);
    let h2 = ScriptHashHead::open(&path).unwrap().expect("live head");
    let mut key = [0u8; 32];
    key[0..8].copy_from_slice(&5u64.to_le_bytes());
    assert!(h2.get(&key).unwrap().is_some());
    {
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len((FILE_HEADER_LEN + 3) as u64)
            .unwrap();
    }
    assert!(matches!(
        ScriptHashHead::open(&path),
        Err(StoreError::Corrupt(_))
    ));
    let _ = std::fs::remove_file(&path);
}

#[test]
fn sh_head_one_key_roundtrip() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-one-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let h = ScriptHashHead::create_with_slots(&path, 64).unwrap();
    let mut key = [0u8; 32];
    key[0] = 3;
    h.insert_many(&[
        (key, ShHeadValue::inline_one(Fk(19))),
        (key, ShHeadValue::inline_one(Fk(20))),
    ])
    .unwrap();
    assert_eq!(
        h.get(&key).unwrap().unwrap(),
        ShHeadValue::inline_one(Fk(20))
    );
    assert_eq!(h.occupied(), 1);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

#[test]
fn occ_sidecar_reads_the_count_and_ignores_short() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shocc-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    store_occ_sidecar(&path, 17).unwrap();
    assert_eq!(load_occ_sidecar(&path), Some(17));
    std::fs::write(occ_sidecar_path(&path), b"SHOCC001").unwrap();
    assert_eq!(load_occ_sidecar(&path), None);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

fn key_in_chunk(chunk: u64, slots: u64, skip: &[[u8; 32]]) -> [u8; 32] {
    for i in 0u64.. {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        if skip.contains(&key) {
            continue;
        }
        if ScriptHashHead::hash_slot(&ScriptHashHead::to_key(&key), slots) / SLOTS_PER_CHUNK
            == chunk
        {
            return key;
        }
    }
    unreachable!("no key hashed to chunk {chunk}")
}

fn chunk_pages(chunk: u64, slots: u64) -> Vec<u64> {
    let base = chunk * SLOTS_PER_CHUNK;
    let n = (slots - base).min(SLOTS_PER_CHUNK);
    let off = ScriptHashHead::slot_file_off(base);
    let len = n * SH_HEAD_SLOT_SIZE as u64;
    let first = off & !4095;
    let last = (off + len - 1) & !4095;
    let mut pages = Vec::new();
    let mut page = first;
    while page <= last {
        pages.push(page);
        page += 4096;
    }
    pages
}

fn zero_pages(path: &std::path::Path, pages: &[u64]) {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    use std::io::{Seek, SeekFrom, Write};
    for &page in pages {
        file.seek(SeekFrom::Start(page)).unwrap();
        file.write_all(&[0u8; 4096]).unwrap();
    }
    file.sync_all().unwrap();
}

/// Zero the 4 KiB blocks the write covers, computed from the keys, not the sidecar.
fn zero_ahead_blocks(path: &std::path::Path, keys: &[[u8; 32]], slots: u64) {
    let mut pages = Vec::new();
    for key in keys {
        let chunk =
            ScriptHashHead::hash_slot(&ScriptHashHead::to_key(key), slots) / SLOTS_PER_CHUNK;
        for page in chunk_pages(chunk, slots) {
            if !pages.contains(&page) {
                pages.push(page);
            }
        }
    }
    zero_pages(path, &pages);
}

#[test]
fn torn_chunk_restores_from_ahead_image() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-ahead-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let keys: Vec<[u8; 32]> = (0u64..101)
        .map(|i| {
            let mut k = [0u8; 32];
            k[0..8].copy_from_slice(&i.to_le_bytes());
            k
        })
        .collect();
    {
        let h = ScriptHashHead::create_with_slots(&path, 256).unwrap();
        let batch: Vec<_> = keys[..100]
            .iter()
            .enumerate()
            .map(|(i, k)| (*k, ShHeadValue::inline_one(Fk(i as u64 + 1))))
            .collect();
        h.insert_many(&batch).unwrap();
        h.flush().unwrap();
        fail_next_scripthash_head_flush_after_ahead(&path);
        assert!(
            h.insert_many(&[(keys[100], ShHeadValue::inline_one(Fk(101)))])
                .is_err(),
            "the failpoint must stop before the chunk pwrite"
        );
    }
    zero_ahead_blocks(&path, &[keys[100]], 256);
    let h = ScriptHashHead::open(&path).unwrap().expect("live head");
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            h.get(key).unwrap(),
            Some(ShHeadValue::inline_one(Fk(i as u64 + 1))),
            "key {i} lost after a torn chunk"
        );
    }
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

#[test]
fn leftover_ahead_pages_survive_a_later_write() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-merge-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let slots = 1024u64;
    let a = key_in_chunk(0, slots, &[]);
    let b = key_in_chunk(3, slots, std::slice::from_ref(&a));
    let c = key_in_chunk(6, slots, &[a, b]);
    let a2 = key_in_chunk(0, slots, &[a, b, c]);
    let b2 = key_in_chunk(3, slots, &[a, b, c, a2]);
    {
        let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
        h.insert_many(&[
            (a, ShHeadValue::inline_one(Fk(1))),
            (b, ShHeadValue::inline_one(Fk(2))),
        ])
        .unwrap();
        h.flush().unwrap();
        fail_next_scripthash_head_flush_on_second_write(&path);
        assert!(
            h.insert_many(&[
                (a2, ShHeadValue::inline_one(Fk(3))),
                (b2, ShHeadValue::inline_one(Fk(4))),
            ])
            .is_err(),
            "the failpoint must stop after the second chunk pwrite"
        );
        fail_next_scripthash_head_flush_after_ahead(&path);
        assert!(
            h.insert_many(&[(c, ShHeadValue::inline_one(Fk(5)))])
                .is_err(),
            "the later write must replace the sidecar"
        );
    }
    zero_pages(&path, &chunk_pages(0, slots));
    let h = ScriptHashHead::open(&path).unwrap().expect("live head");
    assert_eq!(
        h.get(&a).unwrap(),
        Some(ShHeadValue::inline_one(Fk(1))),
        "the committed key on the first page was lost"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

/// A full chunk cache syncs and drops its sidecar. The next wave must not
/// rewrite those pages into the image a failed sync leaves behind.
#[test]
fn cache_eviction_leaves_one_wave_in_the_ahead_image() {
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-wave-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
    let distant = CHUNK_CACHE_MAX as u64 + 50;
    let slots = (distant + 1) * SLOTS_PER_CHUNK;
    let slots = slots.next_power_of_two();
    let mut want = vec![false; distant as usize + 1];
    let mut keys = Vec::with_capacity(CHUNK_CACHE_MAX + 1);
    for i in 0u64.. {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        let chunk =
            ScriptHashHead::hash_slot(&ScriptHashHead::to_key(&key), slots) / SLOTS_PER_CHUNK;
        let hit = (chunk as usize) < CHUNK_CACHE_MAX || chunk == distant;
        if hit && !want[chunk as usize] {
            want[chunk as usize] = true;
            keys.push(key);
            if keys.len() == CHUNK_CACHE_MAX + 1 {
                break;
            }
        }
    }
    let mut wave_pages: Vec<u64> = (0..CHUNK_CACHE_MAX as u64)
        .flat_map(|chunk| chunk_pages(chunk, slots))
        .collect();
    wave_pages.sort_unstable();
    wave_pages.dedup();
    let mut all_pages = wave_pages.clone();
    all_pages.extend(chunk_pages(distant, slots));
    all_pages.sort_unstable();
    all_pages.dedup();
    assert!(
        all_pages.len() > wave_pages.len(),
        "the extra chunk must add a page or the pin cannot see a merged sidecar"
    );
    let batch: Vec<_> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| (*key, ShHeadValue::inline_one(Fk(i as u64 + 1))))
        .collect();
    {
        let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
        let seed = key_in_chunk(CHUNK_CACHE_MAX as u64 + 2, slots, &keys);
        h.insert_many(&[(seed, ShHeadValue::inline_one(Fk(1)))])
            .unwrap();
        h.flush().unwrap();
        let handle = h.file.io_handle();
        struct Disarm(rbitcoin_store::io_handle::IoHandle);
        impl Drop for Disarm {
            fn drop(&mut self) {
                rbitcoin_store::io_handle::disarm_pwrite(self.0);
            }
        }
        let _disarm = Disarm(handle);
        rbitcoin_store::io_handle::arm_fail_sync(handle, 1);
        assert!(
            h.insert_many(&batch).is_err(),
            "the armed sync must fail the wave barrier"
        );
    }
    let mut name = path.file_name().unwrap().to_os_string();
    name.push(".ahead");
    let ahead = path.parent().unwrap().join(name);
    let raw = std::fs::read(&ahead).expect("ahead image");
    let pages = u32::from_le_bytes(raw[8..12].try_into().unwrap()) as usize;
    assert_eq!(
        pages,
        wave_pages.len(),
        "the sidecar kept pages from more than one cache wave"
    );
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&ahead);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}

/// A fill of an empty table writes each run of adjacent slots once, not
/// one write per 24-byte slot. Every key reads back.
#[cfg(debug_assertions)]
#[test]
fn bulk_fill_writes_each_run_of_adjacent_slots_once() {
    use rbitcoin_store::file::{arm_write_trace, take_write_trace, TracedWrite};
    use rbitcoin_store::scripthash_layout::{ShHeadKey, SH_HEAD_KEY_LEN};
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-shhead-runs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let slots = 64u64;
    let h = ScriptHashHead::create_with_slots(&path, slots).unwrap();
    let entries: Vec<(ShHeadKey, ShHeadValue)> = (0u64..40)
        .map(|i| {
            let mut full = [0u8; 32];
            full[..8].copy_from_slice(&i.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
            full[8] = 1;
            (
                ScriptHashHead::to_key(&full),
                ShHeadValue::inline_one(Fk(i + 1)),
            )
        })
        .collect();
    let keys: Vec<ShHeadKey> = entries.iter().map(|(k, _)| *k).collect();
    let mut assigned = ScriptHashHead::assign_empty_slots(slots, &keys).unwrap();
    assigned.sort_unstable();
    assigned.dedup();
    let runs = 1 + assigned.windows(2).filter(|w| w[1] != w[0] + 1).count();
    assert!(runs < assigned.len(), "the fixture has no adjacent slots");
    arm_write_trace();
    h.bulk_fill_empty(&entries).unwrap();
    let slot_writes = take_write_trace()
        .into_iter()
        .filter(|w| {
            matches!(w, TracedWrite::Write { path: p, offset, .. }
                if p == &path && *offset >= FILE_HEADER_LEN as u64)
        })
        .count();
    assert_eq!(slot_writes, runs);
    for (i, (key, val)) in entries.iter().enumerate() {
        let mut full = [0u8; 32];
        full[..SH_HEAD_KEY_LEN].copy_from_slice(key);
        assert_eq!(h.get(&full).unwrap().as_ref(), Some(val), "key {i}");
    }
    drop(h);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(occ_sidecar_path(&path));
}
