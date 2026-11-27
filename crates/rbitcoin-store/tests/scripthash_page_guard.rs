//! Scripthash commits restore torn 4 KiB pages from `store/guard/sh-*`.
use rbitcoin_primitives::Fk;
use rbitcoin_store::error::StoreError;
use rbitcoin_store::file::FILE_HEADER_LEN;
use rbitcoin_store::scripthash::*;
use rbitcoin_store::scripthash_head::{
    load_occ_sidecar, ScriptHashHead, CHUNK_CACHE_MAX, OCC_SCAN_BYTE_CAP, SLOTS_PER_CHUNK,
};
use rbitcoin_store::scripthash_layout::{ShHeadValue, SH_HEAD_SLOT_SIZE};
use rbitcoin_store::scripthash_pages::{SH_PAGE_SIZE, SH_PAGE_STREAM_MAX};
use rbitcoin_store::Store;
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

fn tmp() -> rbitcoin_store::testutil::TempDir {
    rbitcoin_store::testutil::TempDir::labeled("sh-pg").expect("temp dir")
}

fn rec(sh: [u8; 32], tx: u64) -> ScriptHashRecord {
    ScriptHashRecord::from_fk(sh, Fk(tx))
}

fn put_create_batch(t: &ScriptHashTable, recs: impl AsRef<[ScriptHashRecord]>) -> usize {
    t.put_create_batch_append(recs.as_ref()).unwrap().0
}

fn create_fks(t: &ScriptHashTable, sh: &[u8; 32]) -> Vec<u64> {
    t.entries(sh).unwrap().iter().map(|(fk, _)| fk.0).collect()
}

fn page_bytes(path: &Path, off: u64) -> Vec<u8> {
    let mut file = std::fs::File::open(path).unwrap();
    let len = file.metadata().unwrap().len();
    let mut buf = vec![0u8; SH_PAGE_SIZE];
    if off >= len {
        return buf;
    }
    let n = ((len - off) as usize).min(SH_PAGE_SIZE);
    file.seek(SeekFrom::Start(off)).unwrap();
    std::io::Read::read_exact(&mut file, &mut buf[..n]).unwrap();
    buf
}

fn tear_bytes(path: &Path, off: u64, n: usize) {
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(off)).unwrap();
    file.write_all(&vec![0xffu8; n]).unwrap();
    file.sync_all().unwrap();
}

/// The armed `guard/sh-*` marks. A missing `guard/` has none. A
/// `sh-*.tmp` rename leftover is not a mark, since restore skips it. Any
/// other scan error fails the test instead of reading as "no marks".
fn sh_marks(dir: &Path) -> Vec<std::path::PathBuf> {
    let guard = dir.join("guard");
    let rd = match std::fs::read_dir(&guard) {
        Ok(rd) => rd,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => panic!("scan {}: {e}", guard.display()),
    };
    let mut out: Vec<_> = rd
        .map(|e| e.expect("guard/ entry").path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("sh-") && !n.ends_with(".tmp"))
        })
        .collect();
    out.sort();
    out
}

fn neighbor_scripts() -> ([u8; 32], [u8; 32]) {
    let slots = 256u64;
    let mut seen: Vec<(u64, [u8; 32])> = Vec::new();
    for i in 0u16..4096 {
        let sh = script_hash(&i.to_le_bytes());
        let key = rbitcoin_store::scripthash_layout::head_key_from_full(&sh);
        let slot = ScriptHashHead::hash_slot(&key, slots);
        let page = (FILE_HEADER_LEN as u64 + slot * SH_HEAD_SLOT_SIZE as u64) / 4096;
        if let Some((_, other)) = seen.iter().find(|(p, k)| *p == page && *k != sh) {
            return (*other, sh);
        }
        seen.push((page, sh));
    }
    panic!("no two scripts shared an ingest page");
}

/// A suffix unlink whose head-word write mixed a neighbor slot restores it.
#[test]
fn suffix_unlink_restores_a_neighbor_head_word() {
    let dir = tmp();
    let (a, b) = neighbor_scripts();
    let n = SH_PAGE_STREAM_MAX as u64 + 400;
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        let many: Vec<_> = (1..=n).map(|i| rec(a, i)).collect();
        t.put_create_batch_append(&many).unwrap();
        put_create_batch(&t, [rec(b, 1)]);
        t.flush().unwrap();
        let ingest = ingest_path(&dir);
        let off_b = t
            .ingest
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .value_word_offset(&b)
            .unwrap()
            .unwrap();
        t.fail_next_sh_guard_clear();
        let tail: Vec<Fk> = ((n - 399)..=n).map(Fk).collect();
        let err = t.unlink_creates(&a, &tail).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&ingest, off_b, 8);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(sh_marks(&dir).is_empty(), "open removes the mark");
    assert_eq!(create_fks(&t, &b), vec![1]);
    assert_eq!(create_fks(&t, &a).len(), n as usize);
}

/// A slab overwrite that mixed the neighbor slab restores that slab.
#[test]
fn slab_overwrite_restores_the_neighbor_slab() {
    let dir = tmp();
    let a = script_hash(&[0x11]);
    let b = script_hash(&[0x12]);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, [rec(a, 1), rec(a, 2)]);
        put_create_batch(&t, [rec(b, 1), rec(b, 2)]);
        t.flush().unwrap();
        let ShHeadValue::Slab { off: off_b, .. } = t.head_value(&b).unwrap().unwrap() else {
            panic!("two creates must be a slab");
        };
        let body = t.ovf_file().path().to_path_buf();
        t.fail_next_sh_guard_clear();
        let err = t
            .put_create_batch_append(&[rec(a, 3), rec(a, 4)])
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&body, off_b, 16);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert_eq!(create_fks(&t, &b), vec![1, 2]);
    assert_eq!(create_fks(&t, &a), vec![1, 2]);
}

/// A guard that includes page 0 restores a pre-commit free list.
#[test]
fn page0_guard_restores_the_free_list_and_hides_the_bump() {
    let dir = tmp();
    let path = dir.path().to_path_buf();
    let a = script_hash(&[0x21]);
    let b = script_hash(&[0x22]);
    let freed;
    {
        let s = Store::create_tiny(&path).unwrap();
        put_create_batch(&s.scripthash, [rec(a, 1), rec(a, 2)]);
        let ShHeadValue::Slab { off, .. } = s.scripthash.head_value(&a).unwrap().unwrap() else {
            panic!("two creates must be a slab");
        };
        freed = off;
        put_create_batch(&s.scripthash, [rec(a, 3), rec(a, 4)]);
        s.scripthash.flush().unwrap();
        let free = rbitcoin_store::scripthash::read_alloc_header(s.scripthash.ovf_file())
            .unwrap()
            .0
            .free_head[0];
        assert_eq!(free, freed, "class change did not free the old slab");
        s.scripthash.fail_next_sh_guard_clear();
        let err = s
            .scripthash
            .put_create_batch_append(&[rec(b, 10), rec(b, 11)])
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&path).is_empty(), "the crash leaves the mark");
        tear_bytes(s.scripthash.ovf_file().path(), 0, SH_PAGE_SIZE);
        drop(s);
    }
    let s = Store::open_tiny(&path).unwrap();
    assert!(sh_marks(&path).is_empty(), "open removes the mark");
    assert!(
        s.scripthash.head_value(&b).unwrap().is_none(),
        "the head names the discarded bump"
    );
    let free = rbitcoin_store::scripthash::read_alloc_header(s.scripthash.ovf_file())
        .unwrap()
        .0
        .free_head[0];
    assert_eq!(
        free, freed,
        "page 0 did not restore the pre-commit free list"
    );
}

/// A wipe leaves no `store/guard/sh-*`.
#[test]
fn wipe_leaves_no_sh_marks() {
    let dir = tmp();
    let t = ScriptHashTable::create_tiny(&dir).unwrap();
    t.fail_next_sh_guard_clear();
    let err = t
        .put_create_batch_append(&[rec(script_hash(&[0x31]), 1)])
        .unwrap_err();
    assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
    assert!(!sh_marks(&dir).is_empty());
    t.reinit_empty_for_cold_materialize().unwrap();
    assert!(sh_marks(&dir).is_empty(), "wipe left a scripthash mark");
}

/// Sealed overflow with a torn page 0 still opens because restore ran first.
#[test]
fn sealed_ovf_torn_page0_opens_after_restore() {
    let dir = tmp();
    let sh = script_hash(&[0x41]);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, [rec(sh, 1)]);
        let fillers: Vec<_> = (0..203u32)
            .map(|i| {
                rec(
                    script_hash(&[0x42, (i & 0xff) as u8, (i >> 8) as u8]),
                    10 + u64::from(i),
                )
            })
            .collect();
        t.put_create_batch_append(&fillers).unwrap();
        let extra = (0..=255u8)
            .map(|i| script_hash(&[0x43, i]))
            .find(|k| *k != sh)
            .unwrap();
        t.put_create_batch_append(&[rec(extra, 9000)]).unwrap();
        assert!(t.ingest.lock().unwrap().is_none(), "ingest did not seal");
        let sealed = sealed_ovf_path(&dir, 0);
        assert!(sealed.is_file());
        t.fail_next_sh_guard_clear();
        let err = t.put_create_batch_append(&[rec(sh, 2)]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&sealed, 0, 4);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(
        !t.sealed_ovf.lock().unwrap().is_empty(),
        "restore did not run before sealed magic"
    );
    assert_eq!(create_fks(&t, &sh), vec![1]);
}

/// Two new keys that share a hash slot: the displaced slot can sit on the
/// next 4 KiB page. That page is armed before the empty-table write.
#[test]
fn empty_ingest_collision_restores_the_displaced_page() {
    let slots = rbitcoin_store::scripthash::ingest_oa_slots(rbitcoin_store::HeadScale::Tiny);
    let (a, b, page) = displaced_pair(slots);
    let dir = tmp();
    let ingest = ingest_path(&dir);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        t.fail_next_sh_guard_clear();
        let err = t
            .put_create_batch_append(&[rec(a, 1), rec(b, 2)])
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(ingest.is_file(), "the empty fill created ingest");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&ingest, page, SH_PAGE_SIZE);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(sh_marks(&dir).is_empty(), "open removes the mark");
    let got = page_bytes(&ingest, page);
    assert_ne!(
        got,
        vec![0xffu8; SH_PAGE_SIZE],
        "the displaced ingest page stayed torn"
    );
    assert!(
        create_fks(&t, &a).is_empty() && create_fks(&t, &b).is_empty(),
        "open kept keys from the failed commit"
    );
}

/// A non-empty ingest wraps a collision on its last slot to slot 0. Each
/// slot sits in its own chunk and on its own 4 KiB page, so imaging only the
/// primary chunk would leave slot 0 unarmed. The displaced page is armed too.
#[test]
fn nonempty_ingest_collision_restores_the_displaced_page() {
    // Tiny has 256 slots, and its last chunk spans both 4 KiB pages.
    const SLOTS: u64 = 1024;
    let (a, b) = last_slot_pair(SLOTS);
    let prior = (0u32..)
        .map(|i| script_hash(&[0xfe, (i & 0xff) as u8, (i >> 8) as u8]))
        .find(|sh| {
            let slot = ScriptHashHead::hash_slot(&ScriptHashHead::to_key(sh), SLOTS);
            slot > SLOTS_PER_CHUNK && slot < SLOTS - SLOTS_PER_CHUNK
        })
        .unwrap();
    let dir = tmp();
    let ingest = ingest_path(&dir);
    let slot0 = ScriptHashHead::slot_file_off(0);
    let slot0_page = slot0 / SH_PAGE_SIZE as u64 * SH_PAGE_SIZE as u64;
    let before;
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        *t.ingest.lock().unwrap() =
            Some(ScriptHashHead::create_with_slots(&ingest, SLOTS).unwrap());
        put_create_batch(&t, [rec(prior, 7)]);
        before = page_bytes(&ingest, slot0_page);
        t.fail_next_sh_guard_clear();
        let err = t
            .put_create_batch_append(&[rec(a, 1), rec(b, 2)])
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        let word = |sh: &[u8; 32]| {
            t.ingest
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .value_word_offset(sh)
                .unwrap()
                .unwrap()
        };
        let last = ScriptHashHead::slot_file_off(SLOTS - 1);
        let mut offs = [word(&a), word(&b)];
        offs.sort_unstable();
        assert!(
            (slot0..slot0 + SH_HEAD_SLOT_SIZE as u64).contains(&offs[0]),
            "no key wrapped to slot 0: {offs:?}"
        );
        assert!(
            (last..last + SH_HEAD_SLOT_SIZE as u64).contains(&offs[1]),
            "no key kept the last slot: {offs:?}"
        );
        assert_ne!(
            offs[0] / SH_PAGE_SIZE as u64,
            offs[1] / SH_PAGE_SIZE as u64,
            "the displaced slot shares the primary page"
        );
        tear_bytes(&ingest, slot0, SH_HEAD_SLOT_SIZE);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(sh_marks(&dir).is_empty(), "open removes the mark");
    assert_eq!(
        page_bytes(&ingest, slot0_page),
        before,
        "the displaced ingest page stayed torn"
    );
    assert!(create_fks(&t, &a).is_empty() && create_fks(&t, &b).is_empty());
    assert_eq!(create_fks(&t, &prior), vec![7]);
}

/// Two keys whose first probe is the last of `slots`.
fn last_slot_pair(slots: u64) -> ([u8; 32], [u8; 32]) {
    let mut hits = (0u32..200_000)
        .map(|i| script_hash(&i.to_le_bytes()))
        .filter(|sh| ScriptHashHead::hash_slot(&ScriptHashHead::to_key(sh), slots) == slots - 1);
    let a = hits.next().expect("no key hashed to the last slot");
    let b = hits.next().expect("one key hashed to the last slot");
    (a, b)
}

/// `ingest.ahead` is the post-image. Open must not replay it over the mark.
#[test]
fn ahead_sidecar_does_not_override_a_restored_ingest() {
    let dir = tmp();
    let a = script_hash(&[0x51]);
    let b = script_hash(&[0x52]);
    let ingest = ingest_path(&dir);
    let ahead = ingest.with_file_name("ingest.ahead");
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, [rec(a, 1)]);
        rbitcoin_store::scripthash_head::fail_next_scripthash_head_flush_after_ahead(&ingest);
        let err = t.put_create_batch_append(&[rec(b, 2)]).unwrap_err();
        assert!(matches!(err, StoreError::Io { .. }), "{err}");
        assert!(
            format!("{err:?}").contains("injected scripthash head ahead failure"),
            "the commit failed before the ahead write: {err:?}"
        );
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        assert!(ahead.is_file(), "the failed flush published no ahead image");
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(!ahead.exists(), "open kept the ahead image");
    assert!(
        t.head_value(&b).unwrap().is_none(),
        "ahead replay published the failed key"
    );
    assert_eq!(create_fks(&t, &a), vec![1]);
}

/// A batch that crosses the seal load does not publish a sealed file while
/// the page guard is still armed.
#[test]
fn seal_waits_until_the_mark_is_clear() {
    let dir = tmp();
    let prior: Vec<_> = (0..204u32)
        .map(|i| rec(script_hash(&i.to_le_bytes()), 1))
        .collect();
    let new_a = script_hash(&[0x61, 0x01]);
    let new_b = script_hash(&[0x61, 0x02]);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        t.put_create_batch_append(&prior).unwrap();
        assert!(t.ingest.lock().unwrap().is_some(), "204 keys fit");
        t.fail_next_sh_guard_clear();
        let err = t
            .put_create_batch_append(&[rec(new_a, 3), rec(new_b, 4)])
            .unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(t.head_value(&new_a).unwrap().is_none());
    assert!(t.head_value(&new_b).unwrap().is_none());
    assert_eq!(create_fks(&t, &prior[0].scripthash), vec![1]);
}

/// Freeing an extent rewrites every page header. Each of those pages is armed.
#[test]
fn extent_free_restores_an_interior_page() {
    let dir = tmp();
    let a = script_hash(&[0x71]);
    let n = SH_PAGE_STREAM_MAX as u64 + 8;
    let body_off;
    let before;
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        let many: Vec<_> = (1..=n).map(|i| rec(a, i)).collect();
        t.put_create_batch_append(&many).unwrap();
        let ShHeadValue::Extent { last_page } = t.head_value(&a).unwrap().unwrap() else {
            panic!("the key must be an extent");
        };
        let first = paged_first_from_last(t.ovf_file(), last_page).unwrap();
        assert_ne!(first, last_page, "the chain needs an interior page");
        body_off = first;
        let body = t.ovf_file().path().to_path_buf();
        before = page_bytes(&body, first);
        t.fail_next_sh_guard_clear();
        let err = t.unlink_creates(&a, &[Fk(1)]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&body, first, 8);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert_eq!(page_bytes(t.ovf_file().path(), body_off)[..8], before[..8]);
    assert_eq!(create_fks(&t, &a).len(), n as usize);
}

/// A truncated `guard/sh-*.tmp` is a rename leftover, not a mark.
#[test]
fn sh_page_guard_kept_tmp_open() {
    let dir = tmp();
    let path = dir.path().to_path_buf();
    drop(Store::create_tiny(&path).unwrap());
    let guard = path.join("guard");
    std::fs::create_dir_all(&guard).unwrap();
    let torn = guard.join("sh-ovf.tmp");
    std::fs::write(&torn, b"torn").unwrap();
    let s = Store::open_tiny(&path).unwrap();
    assert!(
        !path.join(SH_REBUILD_NAME).exists(),
        "open read the unpublished temp as a mark"
    );
    std::fs::create_dir_all(&guard).unwrap();
    std::fs::write(&torn, b"torn").unwrap();
    s.scripthash.reset_all_bodies().unwrap();
    assert!(!torn.exists(), "reset left the scripthash temp");
}

/// One batch seals on the first key. A later key that already lived in
/// ingest must follow that row into the sealed file.
#[test]
fn sh_page_guard_kept_mid_batch_seal() {
    let dir = tmp();
    let existing = script_hash(&[0x01]);
    let new_key = (0u16..20_000)
        .map(|i| script_hash(&i.to_le_bytes()))
        .find(|k| *k < existing)
        .expect("a key that sorts before the resident one");
    let mut prior = vec![rec(existing, 1)];
    for i in 0u32.. {
        let sh = script_hash(&(10_000u32 + i).to_le_bytes());
        if sh == existing || sh == new_key {
            continue;
        }
        prior.push(rec(sh, 1));
        if prior.len() == 204 {
            break;
        }
    }
    let t = ScriptHashTable::create_tiny(&dir).unwrap();
    t.put_create_batch_append(&prior).unwrap();
    assert_eq!(
        t.ingest.lock().unwrap().as_ref().unwrap().occupied(),
        204,
        "one slot of room before the seal"
    );
    put_create_batch(&t, [rec(new_key, 9), rec(existing, 2)]);
    let ingest_has = t
        .ingest
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|h| h.get(&existing).unwrap())
        .is_some();
    assert!(
        !ingest_has,
        "the resident key was inserted into the new ingest"
    );
    assert_eq!(t.key_home(&existing).unwrap(), KeyHome::SealedOvf);
    assert_eq!(create_fks(&t, &existing), vec![1, 2]);
    assert_eq!(t.key_home(&new_key).unwrap(), KeyHome::SealedOvf);
    assert_eq!(create_fks(&t, &new_key), vec![9]);
}

/// Previewing more than one chunk-cache wave must not publish `Fk(1)` or
/// drop a displaced page from the mark.
#[test]
fn sh_page_guard_kept_preview_chunks() {
    let dir = tmp();
    let slots = 65_536u64;
    let (a, b, chunk, displaced_page) = displaced_in_early_chunk(slots);
    let ingest = ingest_path(&dir);
    let mut seed = [0u8; 32];
    seed[0..8].copy_from_slice(&u64::MAX.to_le_bytes());
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        drop(t);
        let h = ScriptHashHead::create_with_slots(&ingest, slots).unwrap();
        h.insert(&seed, &ShHeadValue::inline_one(Fk(1))).unwrap();
        h.flush().unwrap();
    }
    let mut recs = vec![rec(a, 2), rec(b, 3)];
    for extra in (chunk + 1)..=(chunk + CHUNK_CACHE_MAX as u64) {
        recs.push(rec(
            key_in_chunk(extra, slots, displaced_page, &recs),
            4 + extra,
        ));
    }
    let before;
    {
        let t = ScriptHashTable::open_tiny(&dir).unwrap();
        assert!(t.ingest.lock().unwrap().is_some(), "open kept the head");
        before = std::fs::read(&ingest).unwrap();
        t.fail_next_sh_guard_clear();
        let err = t.put_create_batch_append(&recs).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(
            std::fs::read(&ingest).unwrap() != before,
            "the commit did not write the head"
        );
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        drop(t);
    }
    rbitcoin_store::page_guard::restore_sh_marks(dir.path()).unwrap();
    assert!(
        std::fs::read(&ingest).unwrap() == before,
        "a flushed preview page stayed dirty"
    );
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(t.head_value(&a).unwrap().is_none());
    assert!(t.head_value(&b).unwrap().is_none());
    assert_eq!(
        t.head_value(&seed).unwrap(),
        Some(ShHeadValue::inline_one(Fk(1)))
    );
}

/// The occ sidecar is armed with the slot pages. Open must not keep the
/// count `stage_rewrites` published after the slots were put back.
#[test]
fn sh_page_guard_kept_occ_restore() {
    let dir = tmp();
    let slots = ((OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1).next_power_of_two();
    let ingest = ingest_path(&dir);
    let a = script_hash(&[0xa1]);
    let b = script_hash(&[0xa2]);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        drop(t);
        let h = ScriptHashHead::create_with_slots(&ingest, slots).unwrap();
        h.insert(&a, &ShHeadValue::inline_one(Fk(1))).unwrap();
        h.flush().unwrap();
    }
    {
        let t = ScriptHashTable::open_tiny(&dir).unwrap();
        assert_eq!(t.ingest.lock().unwrap().as_ref().unwrap().occupied(), 1);
        t.fail_next_sh_guard_clear();
        let err = t.put_create_batch_append(&[rec(b, 2)]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert_eq!(load_occ_sidecar(&ingest), Some(2));
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert_eq!(
        t.ingest.lock().unwrap().as_ref().unwrap().occupied(),
        1,
        "open kept the occ count from the failed commit"
    );
    assert!(t.head_value(&b).unwrap().is_none());
    assert_eq!(
        t.head_value(&a).unwrap(),
        Some(ShHeadValue::inline_one(Fk(1)))
    );
}

fn key_in_chunk(chunk: u64, slots: u64, avoid_page: u64, have: &[ScriptHashRecord]) -> [u8; 32] {
    for i in 0u64..2_000_000 {
        let mut sh = [0u8; 32];
        sh[0..8].copy_from_slice(&i.to_le_bytes());
        sh[16..24].copy_from_slice(&chunk.to_le_bytes());
        if have.iter().any(|r| r.scripthash == sh) {
            continue;
        }
        let slot = ScriptHashHead::hash_slot(&ScriptHashHead::to_key(&sh), slots);
        if slot / SLOTS_PER_CHUNK != chunk {
            continue;
        }
        if ScriptHashHead::slot_file_off(slot) / 4096 == avoid_page {
            continue;
        }
        return sh;
    }
    panic!("no key in chunk {chunk} outside page {avoid_page}");
}

/// Two keys share a slot in an early chunk. The next slot sits on another
/// page of that same chunk, so a one-slot probe does not image it.
fn displaced_in_early_chunk(slots: u64) -> ([u8; 32], [u8; 32], u64, u64) {
    let chunks = slots / SLOTS_PER_CHUNK;
    for chunk in 1..chunks.saturating_sub(CHUNK_CACHE_MAX as u64) {
        let mut seen: Vec<(u64, [u8; 32])> = Vec::new();
        for i in 0u64..80_000 {
            let mut sh = [0u8; 32];
            sh[0..8].copy_from_slice(&i.to_le_bytes());
            sh[31] = 0xd0;
            let slot = ScriptHashHead::hash_slot(&ScriptHashHead::to_key(&sh), slots);
            if slot / SLOTS_PER_CHUNK != chunk {
                continue;
            }
            let next = (slot + 1) & (slots - 1);
            if next / SLOTS_PER_CHUNK != chunk {
                continue;
            }
            let page = ScriptHashHead::slot_file_off(slot) / 4096;
            let next_page = ScriptHashHead::slot_file_off(next) / 4096;
            if page == next_page {
                continue;
            }
            if let Some((_, other)) = seen.iter().find(|(s, k)| *s == slot && *k != sh) {
                return (*other, sh, chunk, next_page);
            }
            seen.push((slot, sh));
        }
    }
    panic!("no early chunk held a displaced slot on the next page");
}

fn displaced_pair(slots: u64) -> ([u8; 32], [u8; 32], u64) {
    let mut by_slot: Vec<(u64, [u8; 32])> = Vec::new();
    for i in 0u32..20_000 {
        let sh = script_hash(&i.to_le_bytes());
        let key = rbitcoin_store::scripthash_head::ScriptHashHead::to_key(&sh);
        let slot = rbitcoin_store::scripthash_head::ScriptHashHead::hash_slot(&key, slots);
        let off = rbitcoin_store::scripthash_head::ScriptHashHead::slot_file_off(slot);
        let next = (slot + 1) & (slots - 1);
        let next_off = rbitcoin_store::scripthash_head::ScriptHashHead::slot_file_off(next);
        let page = SH_PAGE_SIZE as u64;
        let covers_next =
            (off / page) == (next_off / page) || ((off + 24 - 1) / page) == (next_off / page);
        if covers_next {
            continue;
        }
        if let Some((_, other)) = by_slot.iter().find(|(s, k)| *s == slot && *k != sh) {
            return (*other, sh, (next_off / page) * page);
        }
        by_slot.push((slot, sh));
    }
    panic!("no two scripts collided onto a slot whose next page is unimaged");
}

/// A commit that fails before its clear leaves `guard/sh-*` armed with the
/// body and ingest images. A seal then would drop the ingest image and keep
/// the body image, so open could roll back a slab the sealed head names.
#[test]
fn seal_refuses_while_a_commit_guard_is_armed() {
    let dir = tmp();
    let t = ScriptHashTable::create_tiny(&dir).unwrap();
    put_create_batch(&t, [rec(script_hash(&[0xb1]), 1)]);
    t.fail_next_sh_guard_clear();
    assert!(t
        .put_create_batch_append(&[rec(script_hash(&[0xb2]), 2)])
        .is_err());
    let marks = sh_marks(&dir);
    assert!(!marks.is_empty(), "the failed clear leaves the mark");
    let before: Vec<Vec<u8>> = marks.iter().map(|m| std::fs::read(m).unwrap()).collect();
    assert!(t.seal_ingest().is_err(), "sealed over an armed guard");
    let after: Vec<Vec<u8>> = sh_marks(&dir)
        .iter()
        .map(|m| std::fs::read(m).unwrap())
        .collect();
    assert_eq!(before, after, "the seal changed the armed mark");
}

/// A page extent after an unaligned bump carves the gap into the free list.
/// Those links share a block with the slab below the bump, so they are
/// written only inside the armed commit. An arm that fails leaves the body
/// unwritten.
#[test]
fn gap_links_wait_for_the_arm() {
    use rbitcoin_store::file::TracedWrite;
    let dir = tmp();
    let t = ScriptHashTable::create_tiny(&dir).unwrap();
    let key = script_hash(&[0xd1]);
    put_create_batch(&t, (1..=3u64).map(|i| rec(key, i)).collect::<Vec<_>>());
    let bodies: Vec<std::path::PathBuf> = t
        .bodies
        .iter()
        .chain(t.ovf_body.iter())
        .map(|b| b.path().to_path_buf())
        .collect();
    // A file where guard/ belongs makes the next arm fail.
    let guard = dir.join("guard");
    let _ = std::fs::remove_dir_all(&guard);
    std::fs::write(&guard, b"not a directory").unwrap();
    let many: Vec<ScriptHashRecord> = (4..=600u64).map(|i| rec(key, i)).collect();
    rbitcoin_store::file::arm_write_trace();
    assert!(t.put_create_batch_append(&many).is_err());
    let trace = rbitcoin_store::file::take_write_trace();
    for w in trace {
        if let TracedWrite::Write { path, offset, len } = w {
            assert!(
                !bodies.contains(&path),
                "a body write before the arm: {}@{offset}+{len}",
                path.display()
            );
        }
    }
}

/// The deferred empty-head commit after a tip shrink writes the head word
/// and the free-list links of the dropped chain. A crash before its clear
/// restores the head and the chain together.
#[test]
fn deferred_empty_commit_restores_the_head_and_chain() {
    let dir = tmp();
    let sh = script_hash(&[0xe1]);
    let n = 300u64;
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, (1..=n).map(|i| rec(sh, i)).collect::<Vec<_>>());
        let fks: Vec<Fk> = (1..=n).map(Fk).collect();
        t.unlink_creates_batch(&[(sh, fks)]).unwrap();
        assert!(t.deferred_empty(&sh));
        t.fail_next_sh_guard_clear();
        assert!(t.commit_deferred_empty_heads().is_err());
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert_eq!(
        create_fks(&t, &sh),
        (1..=n).collect::<Vec<_>>(),
        "open did not restore the head and chain the empty commit wrote"
    );
}

/// Two keys in one ingest chunk, on either side of a 4 KiB boundary.
fn chunk_straddling_scripts() -> ([u8; 32], [u8; 32]) {
    let slots = 256u64;
    let (mut low, mut high) = (None, None);
    for i in 0u16..8192 {
        let sh = script_hash(&i.to_le_bytes());
        let slot = ScriptHashHead::hash_slot(&ScriptHashHead::to_key(&sh), slots);
        if slot / SLOTS_PER_CHUNK != 1 {
            continue;
        }
        let first = ScriptHashHead::slot_file_off(slot) / 4096;
        let last = (ScriptHashHead::slot_file_off(slot) + SH_HEAD_SLOT_SIZE as u64 - 1) / 4096;
        match (first, last) {
            (0, 0) if low.is_none() => low = Some(sh),
            (1, 1) if high.is_none() => high = Some(sh),
            _ => {}
        }
        if let (Some(a), Some(b)) = (low, high) {
            return (a, b);
        }
    }
    panic!("no ingest chunk held keys on both of its pages");
}

/// An ingest unlink rewrites the whole chunk that holds its key. A torn
/// write to that chunk's other page restores from the mark.
#[test]
fn ingest_unlink_restores_the_other_page_of_its_chunk() {
    let dir = tmp();
    let (a, b) = chunk_straddling_scripts();
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, (1..=3u64).map(|i| rec(a, i)).collect::<Vec<_>>());
        put_create_batch(&t, [rec(b, 9)]);
        t.flush().unwrap();
        let ingest = ingest_path(&dir);
        let (off_a, off_b) = {
            let g = t.ingest.lock().unwrap();
            let h = g.as_ref().unwrap();
            (
                h.value_word_offset(&a).unwrap().unwrap(),
                h.value_word_offset(&b).unwrap().unwrap(),
            )
        };
        assert_ne!(off_a / 4096, off_b / 4096, "the keys share a page");
        t.fail_next_sh_guard_clear();
        let err = t.unlink_creates_batch(&[(a, vec![Fk(1)])]).unwrap_err();
        assert!(matches!(err, StoreError::Corrupt(_)), "{err}");
        assert!(!sh_marks(&dir).is_empty(), "the crash leaves the mark");
        tear_bytes(&ingest, off_b, 8);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(sh_marks(&dir).is_empty(), "open removes the mark");
    assert_eq!(create_fks(&t, &b), vec![9]);
    assert_eq!(create_fks(&t, &a), vec![1, 2, 3]);
}

/// A deferred empty-head commit whose clear failed keeps its first mark.
/// The retry clears every image that arm took, not only the body.
#[test]
fn deferred_empty_retry_clears_the_first_mark() {
    let dir = tmp();
    let sh = script_hash(&[0xe2]);
    let n = 300u64;
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, (1..=n).map(|i| rec(sh, i)).collect::<Vec<_>>());
        let fks: Vec<Fk> = (1..=n).map(Fk).collect();
        t.unlink_creates_batch(&[(sh, fks)]).unwrap();
        t.fail_next_sh_guard_clear();
        assert!(t.commit_deferred_empty_heads().is_err());
        assert!(
            !sh_marks(&dir).is_empty(),
            "the failed clear leaves the mark"
        );
        t.commit_deferred_empty_heads().unwrap();
        assert!(!t.deferred_empty(&sh));
        assert!(sh_marks(&dir).is_empty(), "the retry left an image armed");
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(
        create_fks(&t, &sh).is_empty(),
        "open restored a freed chain"
    );
}

fn sealed_segment_files(dir: &Path) -> usize {
    std::fs::read_dir(dir.join("scripthash.ovf"))
        .unwrap()
        .flatten()
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.len() == 6 && name.chars().all(|c| c.is_ascii_digit())
        })
        .count()
}

/// A seal whose mark-name drop fails keeps each key in one place. The
/// rejected segment's removal is synced before the drop error returns, so
/// a crash cannot bring it back beside ingest. The retry seals ingest once,
/// not a second segment of the same keys.
#[test]
fn seal_that_fails_to_drop_the_ingest_name_keeps_one_copy() {
    let dir = tmp();
    let keys: Vec<[u8; 32]> = (0u8..4).map(|i| script_hash(&[0xc0, i])).collect();
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        for (i, k) in keys.iter().enumerate() {
            put_create_batch(&t, [rec(*k, i as u64 + 1)]);
        }
        t.fail_next_ingest_rel_drop();
        rbitcoin_store::file::arm_fsync_dir_trace();
        let err = t.seal_ingest().unwrap_err();
        let synced = rbitcoin_store::file::take_fsynced_dirs();
        assert!(
            err.to_string().contains("ingest name drop"),
            "the drop error was replaced: {err}"
        );
        assert_eq!(sealed_segment_files(&dir), 0, "the rejected segment stayed");
        // Windows has no directory fsync, so nothing is recorded there.
        let ovf_syncs = synced
            .iter()
            .filter(|p| **p == dir.join("scripthash.ovf"))
            .count();
        assert!(
            !cfg!(unix) || ovf_syncs >= 2,
            "the segment's .idx rename syncs the directory once; its removal \
             must sync it again: {synced:?}"
        );
        t.seal_ingest().unwrap();
        assert_eq!(t.sealed_ovf.lock().unwrap().len(), 1, "keys sealed twice");
        assert!(
            t.ingest.lock().unwrap().is_none(),
            "ingest stayed installed"
        );
        t.flush().unwrap();
    }
    assert_eq!(sealed_segment_files(&dir), 1, "a second segment on disk");
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(create_fks(&t, k), vec![i as u64 + 1]);
    }
}

/// A sibling commit's clear drops every record of the files it synced,
/// including a failed job's images. That job's retry arms again before it
/// writes a free-list link.
#[test]
fn deferred_empty_retry_rearms_after_a_sibling_clear() {
    let dir = tmp();
    let a = script_hash(&[0xe3]);
    let b = script_hash(&[0xe4]);
    let n = 300u64;
    let t = ScriptHashTable::create_tiny(&dir).unwrap();
    put_create_batch(&t, (1..=n).map(|i| rec(a, i)).collect::<Vec<_>>());
    put_create_batch(&t, (1..=n).map(|i| rec(b, n + i)).collect::<Vec<_>>());
    let fks_a: Vec<Fk> = (1..=n).map(Fk).collect();
    let fks_b: Vec<Fk> = (n + 1..=2 * n).map(Fk).collect();
    t.unlink_creates_batch(&[(a, fks_a), (b, fks_b)]).unwrap();
    ScriptHashTable::fail_next_deferred_free();
    assert!(t.commit_deferred_empty_heads().is_err());
    assert!(t.deferred_empty(&a) != t.deferred_empty(&b));
    ScriptHashTable::fail_next_deferred_free();
    assert!(t.commit_deferred_empty_heads().is_err());
    assert!(
        !sh_marks(&dir).is_empty(),
        "the retry wrote under no mark after the sibling cleared it"
    );
    t.commit_deferred_empty_heads().unwrap();
    assert!(sh_marks(&dir).is_empty(), "the retry left an image armed");
    drop(t);
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(create_fks(&t, &a).is_empty());
    assert!(create_fks(&t, &b).is_empty());
}

/// A commit that failed before its clear leaves its pre-images armed. A
/// later commit in the same process must keep them, not re-image those
/// pages with the failed commit's bytes: a crash during the later commit
/// then restores both to the state before the first.
#[test]
fn a_later_commit_keeps_an_armed_pre_image() {
    let dir = tmp();
    let a = script_hash(&[0x61]);
    let b = script_hash(&[0x62]);
    {
        let t = ScriptHashTable::create_tiny(&dir).unwrap();
        put_create_batch(&t, [rec(a, 1), rec(a, 2)]);
        put_create_batch(&t, [rec(b, 1), rec(b, 2)]);
        t.flush().unwrap();
        // Each append must reach its guard clear, so both commits run
        // under the armed mark and their lists are live before the crash.
        let cleared = |err: StoreError| {
            assert!(
                matches!(
                    err,
                    StoreError::Corrupt("scripthash page guard clear failed")
                ),
                "{err}"
            );
        };
        t.fail_next_sh_guard_clear();
        cleared(
            t.put_create_batch_append(&[rec(a, 3), rec(a, 4)])
                .unwrap_err(),
        );
        t.fail_next_sh_guard_clear();
        cleared(
            t.put_create_batch_append(&[rec(b, 3), rec(b, 4)])
                .unwrap_err(),
        );
        assert_eq!(create_fks(&t, &a), vec![1, 2, 3, 4]);
        assert_eq!(create_fks(&t, &b), vec![1, 2, 3, 4]);
        drop(t);
    }
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    assert!(sh_marks(&dir).is_empty(), "open removes the marks");
    assert_eq!(create_fks(&t, &a), vec![1, 2]);
    assert_eq!(create_fks(&t, &b), vec![1, 2]);
}

/// A large ingest (slots past page 0) with `a` holding two creates and `b`
/// one, committed through the table.
fn large_ingest_with_keys(dir: &Path) -> ([u8; 32], [u8; 32]) {
    let slots = ((OCC_SCAN_BYTE_CAP / SH_HEAD_SLOT_SIZE as u64) + 1).next_power_of_two();
    let ingest = ingest_path(dir);
    let a = script_hash(&[0xb1]);
    let b = script_hash(&[0xb2]);
    {
        let t = ScriptHashTable::create_tiny(dir).unwrap();
        drop(t);
        let h = ScriptHashHead::create_with_slots(&ingest, slots).unwrap();
        h.insert(&script_hash(&[0xb0]), &ShHeadValue::inline_one(Fk(1)))
            .unwrap();
        h.flush().unwrap();
    }
    let t = ScriptHashTable::open_tiny(dir).unwrap();
    put_create_batch(&t, [rec(a, 2), rec(a, 3), rec(b, 4)]);
    t.flush().unwrap();
    assert!(sh_marks(dir).is_empty());
    (a, b)
}

/// The unlink rewrites a slot far from the header page. Its flush must not
/// be refused, and its mark must clear.
#[test]
fn an_unlink_on_a_large_ingest_commits_and_clears() {
    let dir = tmp();
    let (a, b) = large_ingest_with_keys(&dir);
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    t.unlink_creates_batch(&[(a, vec![Fk(3)])]).unwrap();
    assert_eq!(create_fks(&t, &a), vec![2]);
    assert_eq!(create_fks(&t, &b), vec![4]);
    assert!(sh_marks(&dir).is_empty(), "the unlink left its mark");
}

/// A large ingest opened without `.occ` has an unknown count and writes no
/// sidecar. A mark must not wait on the missing file, or every later seal
/// waits on it.
#[test]
fn an_unlink_on_an_ingest_without_occ_clears_its_mark() {
    let dir = tmp();
    let (a, b) = large_ingest_with_keys(&dir);
    let occ = rbitcoin_store::scripthash_head::occ_sidecar_path(&ingest_path(&dir));
    std::fs::remove_file(&occ).unwrap();
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    t.unlink_creates_batch(&[(a, vec![Fk(3)])]).unwrap();
    assert_eq!(create_fks(&t, &a), vec![2]);
    assert!(
        !occ.exists(),
        "the unlink wrote a sidecar for an unknown count"
    );
    assert!(sh_marks(&dir).is_empty(), "the unlink left its mark");
    put_create_batch(&t, [rec(script_hash(&[0xb3]), 5)]);
    assert!(sh_marks(&dir).is_empty(), "the append left its mark");
    assert_eq!(create_fks(&t, &b), vec![4]);
}

/// A suffix unlink that moves a large ingest's head word syncs the head
/// under its mark. That sync must not be refused.
#[test]
fn a_suffix_unlink_on_a_large_ingest_commits_and_clears() {
    let dir = tmp();
    let (a, b) = large_ingest_with_keys(&dir);
    let n = SH_PAGE_STREAM_MAX as u64 + 400;
    let t = ScriptHashTable::open_tiny(&dir).unwrap();
    let many: Vec<_> = (10..10 + n).map(|i| rec(a, i)).collect();
    t.put_create_batch_append(&many).unwrap();
    let tail: Vec<Fk> = ((10 + n - 400)..10 + n).map(Fk).collect();
    t.unlink_creates(&a, &tail).unwrap();
    assert_eq!(create_fks(&t, &a).len(), 2 + n as usize - 400);
    assert_eq!(create_fks(&t, &b), vec![4]);
    assert!(sh_marks(&dir).is_empty(), "the unlink left its mark");
}
