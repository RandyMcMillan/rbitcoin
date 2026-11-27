use rbitcoin_primitives::Fk;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::hashhead::*;

fn tmp_path() -> std::path::PathBuf {
    let dir = rbitcoin_store::testutil::fresh_path("hh");
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("header.head")
}

#[test]
fn dense_load_inserts_until_full_without_rehash() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
    for i in 0u64..50 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        h.insert(&key, Fk(i + 1)).unwrap();
    }
    for i in 0u64..50 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
    }
    let mut overflow = None;
    for i in 50u64..70 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        match h.insert(&key, Fk(i + 1)) {
            Ok(_) => continue,
            Err(e) => {
                overflow = Some(e);
                break;
            }
        }
    }
    let err = overflow.expect("64-slot head must refuse past 7/8");
    assert!(
        matches!(err, StoreError::Corrupt(HASH_HEAD_FULL)),
        "got {err}"
    );
    assert_eq!(h.slots(), 64);
    assert_eq!(h.get(&[0u8; 32]).unwrap(), Some(Fk(1)));
    cleanup_hh(&path);
}

fn key_in_chunk(chunk: u64, slots: u64, skip: &[[u8; 32]]) -> [u8; 32] {
    for i in 0u64.. {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        if skip.contains(&key) {
            continue;
        }
        if HashHead::hash_slot(&head_key_prefix(&key), slots) / SLOTS_PER_CHUNK == chunk {
            return key;
        }
    }
    unreachable!("no key hashed to chunk {chunk}")
}

fn chunk_pages(chunk: u64, slots: u64) -> Vec<u64> {
    let base = chunk * SLOTS_PER_CHUNK;
    let n = (slots - base).min(SLOTS_PER_CHUNK);
    let off = HashHead::slot_file_off(base);
    let len = n * SLOT_SIZE as u64;
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
        let chunk = HashHead::hash_slot(&head_key_prefix(key), slots) / SLOTS_PER_CHUNK;
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
    let path = tmp_path();
    let keys: Vec<[u8; 32]> = (0u64..101)
        .map(|i| {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            key
        })
        .collect();
    {
        let h = HashHead::create_with_slots(&path, 256).unwrap();
        let batch: Vec<_> = keys[..100]
            .iter()
            .enumerate()
            .map(|(i, key)| (*key, Fk(i as u64 + 1)))
            .collect();
        h.insert_many(&batch).unwrap();
        h.flush().unwrap();
        fail_next_hash_head_flush_after_ahead(&path);
        assert!(
            h.insert_many(&[(keys[100], Fk(101))]).is_err(),
            "the failpoint must stop before the chunk pwrite"
        );
    }
    zero_ahead_blocks(&path, &[keys[100]], 256);
    let h = HashHead::open(&path).unwrap();
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            h.get(key).unwrap(),
            Some(Fk(i as u64 + 1)),
            "key {i} lost after a torn chunk"
        );
    }
    let mark = path.parent().unwrap().join("guard/header.head");
    assert!(
        !mark.exists(),
        "open must remove the applied header.head mark"
    );
    cleanup_hh(&path);
}

#[test]
fn leftover_ahead_pages_survive_a_later_write() {
    let path = tmp_path();
    let slots = 1024u64;
    let a = key_in_chunk(0, slots, &[]);
    let b = key_in_chunk(3, slots, std::slice::from_ref(&a));
    let c = key_in_chunk(6, slots, &[a, b]);
    let a2 = key_in_chunk(0, slots, &[a, b, c]);
    let b2 = key_in_chunk(3, slots, &[a, b, c, a2]);
    {
        let h = HashHead::create_with_slots(&path, slots).unwrap();
        h.insert_many(&[(a, Fk(1)), (b, Fk(2))]).unwrap();
        h.flush().unwrap();
        fail_next_hash_head_flush_on_second_write(&path);
        assert!(
            h.insert_many(&[(a2, Fk(3)), (b2, Fk(4))]).is_err(),
            "the failpoint must stop after the second chunk pwrite"
        );
        fail_next_hash_head_flush_after_ahead(&path);
        assert!(
            h.insert_many(&[(c, Fk(5))]).is_err(),
            "the later write must replace the sidecar"
        );
    }
    zero_pages(&path, &chunk_pages(0, slots));
    let h = HashHead::open(&path).unwrap();
    assert_eq!(
        h.get(&a).unwrap(),
        Some(Fk(1)),
        "the committed key on the first page was lost"
    );
    cleanup_hh(&path);
}

/// `.mlt` only appends. Its rows and length are durable before the head mark
/// arms, so the mark images `header.head` alone.
#[test]
fn an_mlt_append_is_durable_before_a_head_mark_that_names_only_the_head() {
    let path = tmp_path();
    let mut k1 = [0u8; 32];
    k1[0] = 0xab;
    k1[15] = 1;
    k1[16] = 1;
    let mut k2 = k1;
    k2[16] = 2;
    {
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.insert(&k1, Fk(10)).unwrap();
        h.flush().unwrap();
        fail_next_hash_head_flush_after_ahead(&path);
        assert!(
            h.insert(&k2, Fk(20)).is_err(),
            "the failpoint must stop before the head write"
        );
    }
    let mark = std::fs::read(path.parent().unwrap().join("guard/header.head")).unwrap();
    assert!(
        !mark.windows(4).any(|w| w == b".mlt"),
        "the head mark must not image header.head.mlt"
    );
    assert_eq!(
        *MultiList::open(&path).unwrap().count.lock().unwrap(),
        2,
        "the .mlt rows must be published before the head mark arms"
    );
    zero_ahead_blocks(&path, &[k1, k2], DEFAULT_SLOTS);
    let h = HashHead::open(&path).unwrap();
    let all = h.get_all(&k1).unwrap();
    assert!(
        all.contains(&Fk(10)) && all.contains(&Fk(20)),
        "the head mark and the durable .mlt must give both values, got {all:?}"
    );
    cleanup_hh(&path);
}

/// A head flush whose `.mlt` length flush fails may have made that length
/// durable. The `.mlt` refuses later inserts until reopen, which reads the
/// rows the durable length names.
#[cfg(debug_assertions)]
#[test]
fn a_failed_mlt_length_flush_refuses_inserts_until_reopen() {
    let path = tmp_path();
    let mut k1 = [0u8; 32];
    k1[0] = 0xab;
    k1[15] = 1;
    k1[16] = 1;
    let mut k2 = k1;
    k2[16] = 2;
    let mut k3 = k1;
    k3[16] = 3;
    {
        let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
        h.insert(&k1, Fk(10)).unwrap();
        h.flush().unwrap();
        rbitcoin_store::io_handle::arm_fail_flush(h.multi.file.io_handle(), 1);
        assert!(h.insert(&k2, Fk(20)).is_err());
        assert!(
            h.insert(&k3, Fk(30)).is_err(),
            "an insert after a failed .mlt flush was accepted"
        );
    }
    let h = HashHead::open(&path).unwrap();
    assert!(h.get_all(&k1).unwrap().contains(&Fk(10)));
    h.insert(&k3, Fk(30)).expect("an insert after reopen");
    h.flush().unwrap();
    assert!(h.get_all(&k1).unwrap().contains(&Fk(30)));
    cleanup_hh(&path);
}

#[test]
fn persist_survives_reopen() {
    let path = tmp_path();
    {
        let h = HashHead::create_with_slots(&path, 256).unwrap();
        let mut batch = Vec::new();
        for i in 0u64..100 {
            let mut key = [0u8; 32];
            key[0..8].copy_from_slice(&i.to_le_bytes());
            batch.push((key, Fk(i + 1)));
        }
        h.insert_many(&batch).unwrap();
        h.flush().unwrap();
    }
    let h = HashHead::open(&path).unwrap();
    assert_eq!(h.occupied(), 100);
    for i in 0u64..100 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
    }
    cleanup_hh(&path);
}

#[test]
fn bulk_fill_empty_roundtrip() {
    // Cold materialize path: empty table + pre-size + one insert_many.
    let path = tmp_path();
    // 20_000 keys at load 7/8 need 32_768 slots. create_with_slots is the live presize.
    let h = HashHead::create_with_slots(&path, 32_768).unwrap();
    let mut batch = Vec::new();
    for i in 0u64..20_000 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        batch.push((key, Fk(i + 1)));
    }
    assert_eq!(h.occupied(), 0);
    h.insert_many(&batch).unwrap();
    assert_eq!(h.occupied(), 20_000);
    for i in [0u64, 1, 9999, 19_999] {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        assert_eq!(h.get(&key).unwrap(), Some(Fk(i + 1)));
    }
    // Second wave uses RMW path (non-empty).
    let mut more = Vec::new();
    for i in 20_000u64..21_000 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&i.to_le_bytes());
        more.push((key, Fk(i + 1)));
    }
    h.insert_many(&more).unwrap();
    assert_eq!(h.occupied(), 21_000);
    assert_eq!(h.get(&[0u8; 32]).unwrap(), Some(Fk(1)));
    cleanup_hh(&path);
}

#[test]
fn create_with_slots_sparse_presize() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 1024).unwrap();
    assert_eq!(h.state.lock().unwrap().slots, 1024);
    assert_eq!(h.occupied(), 0);
    h.insert(&[1u8; 32], Fk(1)).unwrap();
    assert_eq!(h.get(&[1u8; 32]).unwrap(), Some(Fk(1)));
    cleanup_hh(&path);
}

#[test]
fn mainnet_scale_slot_targets() {
    assert_eq!(HeadScale::Tiny.initial_slots(), 64);
    assert_eq!(HeadScale::Mainnet.initial_slots(), 1 << 22);
    assert_eq!(HeadScale::Tiny.sh_main_shards(), 1);
    assert_eq!(HeadScale::Mainnet.sh_main_shards(), SH_MAIN_SHARDS_MAINNET);
    assert_eq!(sh_main_shard_count(HeadScale::Tiny), 1);
    assert_eq!(
        sh_main_shard_count(HeadScale::Mainnet),
        SH_MAIN_SHARDS_MAINNET
    );
}

#[test]
fn open_does_not_require_full_ram_copy() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
    h.insert(&[1u8; 32], Fk(42)).unwrap();
    h.flush().unwrap();
    drop(h);
    let h = HashHead::open(&path).unwrap();
    assert_eq!(h.get(&[1u8; 32]).unwrap(), Some(Fk(42)));
    cleanup_hh(&path);
}

#[test]
fn slot_sorted_batch_matches_sequential_inserts() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 1024).unwrap();
    let mut batch = Vec::new();
    for i in 0u64..500 {
        let mut key = [0u8; 32];
        // Scramble key so primary slots are not insertion order.
        key[0..8].copy_from_slice(&(i.wrapping_mul(0x9e3779b97f4a7c15)).to_le_bytes());
        batch.push((key, Fk(i + 1)));
    }
    h.insert_many(&batch).unwrap();
    for (key, fk) in &batch {
        assert_eq!(h.get(key).unwrap(), Some(*fk));
    }
    let mut upd = Vec::new();
    for i in 0u64..50 {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&(i.wrapping_mul(0x9e3779b97f4a7c15)).to_le_bytes());
        upd.push((key, Fk(10_000 + i)));
    }
    h.insert_many(&upd).unwrap();
    for (key, fk) in &upd {
        assert_eq!(h.get(key).unwrap(), Some(*fk));
    }
    assert_eq!(h.occupied(), 500);
    cleanup_hh(&path);
}

fn cleanup_hh(path: &std::path::Path) {
    if let Some(dir) = path.parent() {
        let name = dir.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("rbitcoin-hh-") {
            let _ = std::fs::remove_dir_all(dir);
            return;
        }
    }
    let _ = std::fs::remove_file(path);
    let mut mlt = path.as_os_str().to_os_string();
    mlt.push(".mlt");
    let _ = std::fs::remove_file(mlt);
    discard_grow_part(path);
}

#[test]
fn prefix_collision_multi_list() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
    // Same first 16 bytes, different trailing bytes.
    let mut k1 = [0u8; 32];
    k1[0] = 0xab;
    k1[15] = 1;
    k1[16] = 1;
    let mut k2 = k1;
    k2[16] = 2;
    h.insert(&k1, Fk(10)).unwrap();
    h.insert(&k2, Fk(20)).unwrap();
    assert_eq!(h.occupied(), 1); // one prefix slot
    let all1 = h.get_all(&k1).unwrap();
    let all2 = h.get_all(&k2).unwrap();
    assert_eq!(all1, all2);
    assert!(all1.contains(&Fk(10)) && all1.contains(&Fk(20)));
    assert_eq!(all1[0], Fk(20));
    assert_eq!(h.get(&k1).unwrap(), Some(Fk(20)));
    cleanup_hh(&path);
}

#[test]
fn bip30_same_full_key_multi() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
    let key = [0x42u8; 32];
    h.insert(&key, Fk(1)).unwrap();
    h.insert(&key, Fk(2)).unwrap(); // same txid, second Class A row
    let all = h.get_all(&key).unwrap();
    assert_eq!(all, vec![Fk(2), Fk(1)]);
    assert_eq!(h.occupied(), 1);
    h.insert(&key, Fk(2)).unwrap();
    assert_eq!(h.get_all(&key).unwrap(), vec![Fk(2), Fk(1)]);
    h.flush().unwrap();
    let h2 = HashHead::open(&path).unwrap();
    assert_eq!(h2.get_all(&key).unwrap(), vec![Fk(2), Fk(1)]);
    cleanup_hh(&path);
}

#[test]
fn multi_list_survives_when_head_is_full() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, DEFAULT_SLOTS).unwrap();
    let mut k1 = [0u8; 32];
    k1[0..4].copy_from_slice(&1u32.to_le_bytes());
    let mut k2 = k1;
    k2[20] = 9;
    h.insert(&k1, Fk(100)).unwrap();
    h.insert(&k2, Fk(200)).unwrap();
    let mut n = 0u64;
    loop {
        let mut key = [0u8; 32];
        key[0..8].copy_from_slice(&(n + 1000).to_le_bytes());
        match h.insert(&key, Fk(n + 1)) {
            Ok(_) => n += 1,
            Err(StoreError::Corrupt(HASH_HEAD_FULL)) => break,
            Err(e) => panic!("unexpected {e}"),
        }
        assert!(n < 64, "must refuse before rewriting slots");
    }
    assert_eq!(h.slots(), 64);
    let all = h.get_all(&k1).unwrap();
    assert!(all.contains(&Fk(100)) && all.contains(&Fk(200)));
    cleanup_hh(&path);
}

#[test]
fn head_scale_prefix_and_pack_helpers() {
    assert_eq!(HeadScale::Tiny.initial_slots(), DEFAULT_SLOTS);
    assert_eq!(HeadScale::Mainnet.initial_slots(), 1 << 22);
    assert_eq!(
        sh_main_shard_count(HeadScale::Mainnet),
        SH_MAIN_SHARDS_MAINNET
    );
    assert_eq!(initial_slots_for(HeadScale::Tiny), DEFAULT_SLOTS);
    assert_eq!(initial_slots_for(HeadScale::Mainnet), 1 << 22);
    let full = [0xABu8; 32];
    let p = head_key_prefix(&full);
    assert_eq!(&p[..], &full[..16]);
    assert_eq!(pack_sole(Fk(7)), 7);
    let pm = pack_multi(Fk(3));
    assert_ne!(pm & MULTI_BIT, 0);
    let (multi, fk) = unpack_value(pm);
    assert!(multi);
    assert_eq!(fk, Fk(3));
    let (multi, fk) = unpack_value(42);
    assert!(!multi);
    assert_eq!(fk, Fk(42));
}

#[test]
fn rewrite_to_slots_replaces_file_keeps_keys() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 32).unwrap();
    let mut k_multi = [0u8; 32];
    k_multi[0] = 0x11;
    h.insert(&k_multi, Fk(1)).unwrap();
    h.insert(&k_multi, Fk(2)).unwrap();
    let mut k_sole = [0u8; 32];
    k_sole[0] = 0x22;
    h.insert(&k_sole, Fk(3)).unwrap();
    h.flush().unwrap();
    #[cfg(unix)]
    let old = std::fs::File::open(&path).unwrap();
    #[cfg(unix)]
    let old_ino = {
        use std::os::unix::fs::MetadataExt;
        old.metadata().unwrap().ino()
    };
    let h = h.rewrite_to_slots(64).unwrap();
    assert_eq!(h.slots(), 64);
    assert_eq!(h.get_all(&k_multi).unwrap(), vec![Fk(2), Fk(1)]);
    assert_eq!(h.get(&k_sole).unwrap(), Some(Fk(3)));
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        assert_ne!(
            std::fs::metadata(&path).unwrap().ino(),
            old_ino,
            "open-grow must replace the OA file, not zero the live inode"
        );
        drop(old);
    }
    cleanup_hh(&path);
}

#[test]
fn rewrite_to_slots_discards_leftover_grow_keeps_old_keys() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 32).unwrap();
    let mut k = [0u8; 32];
    k[0] = 0x33;
    h.insert(&k, Fk(9)).unwrap();
    h.flush().unwrap();
    let grow = grow_part_path(&path);
    std::fs::write(&grow, b"stale-grow").unwrap();
    let h = h.rewrite_to_slots(64).unwrap();
    assert_eq!(h.slots(), 64);
    assert_eq!(h.get(&k).unwrap(), Some(Fk(9)));
    assert!(
        !grow.exists(),
        "leftover .grow must not remain after rewrite"
    );
    cleanup_hh(&path);
}

#[test]
fn hashhead_empty_insert_get_miss_flush_and_reopen_multi() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 16).unwrap();
    h.insert_many(&[]).unwrap();
    assert!(h.get(&[0u8; 32]).unwrap().is_none());
    assert!(h.get_all(&[0u8; 32]).unwrap().is_empty());
    let key = [0x55u8; 32];
    h.insert(&key, Fk(1)).unwrap();
    h.insert(&key, Fk(2)).unwrap();
    h.insert(&key, Fk(3)).unwrap();
    assert_eq!(h.get_all(&key).unwrap(), vec![Fk(3), Fk(2), Fk(1)]);
    assert_eq!(h.get(&key).unwrap(), Some(Fk(3)));
    h.flush().unwrap();
    h.flush_async().unwrap();
    drop(h);
    let h = HashHead::open(&path).unwrap();
    assert_eq!(h.get_all(&key).unwrap(), vec![Fk(3), Fk(2), Fk(1)]);
    let mut mlt = path.as_os_str().to_os_string();
    mlt.push(".mlt");
    assert!(std::path::PathBuf::from(mlt).exists());
    cleanup_hh(&path);
}

#[test]
fn multi_list_get_reads_create_and_next() {
    let path = tmp_path();
    let list = MultiList::create(&path).unwrap();
    let create = Fk(0x0102_0304_0506_0708);
    let a = list.append(create, Fk::NULL).unwrap();
    let b = list.append(Fk(9), a).unwrap();
    assert_eq!(list.get(a).unwrap(), (create, Fk::NULL));
    assert_eq!(list.get(b).unwrap(), (Fk(9), a));
    match list.get(Fk(99)) {
        Err(StoreError::NotFound) => {}
        other => panic!("missing multi row must be NotFound, got {other:?}"),
    }
    cleanup_hh(&path);
}

#[test]
fn hashhead_one_key_roundtrip() {
    let path = tmp_path();
    let h = HashHead::create_with_slots(&path, 64).unwrap();
    let mut key = [0u8; 32];
    key[0] = 9;
    h.insert_many(&[(key, Fk(77)), (key, Fk(78))]).unwrap();
    assert_eq!(h.get_all(&key).unwrap(), vec![Fk(78), Fk(77)]);
    assert_eq!(h.occupied(), 1);
    cleanup_hh(&path);
}

/// A bare file name has no directory for `guard/header.head`. Open refuses
/// it, as the arm and the clear do, instead of skipping the restore.
#[test]
fn open_refuses_a_bare_file_name() {
    match HashHead::open("hashhead-bare-name") {
        Err(StoreError::Rejected(_)) => {}
        Err(e) => panic!("bare name opened past the guard: {e:?}"),
        Ok(_) => panic!("bare name opened"),
    }
}
