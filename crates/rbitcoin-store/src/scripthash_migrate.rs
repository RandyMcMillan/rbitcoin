//! One-time migration: v3 linked-list scripthash → hybrid inline/slab (schema 4).
//!
//! Touches only `scripthash.head` + `scripthash.body`. `scripthash.runs/` is left
//! untouched; materialize before or after migrate uses the live table format.
//!
//! **Bulk path:** walk v3 chains once, pack directly into a pre-sized hybrid body/head
//! (no per-key `put_create_batch` / alloc-header RMW). Prefer loading the v3 body into
//! RAM when it fits so chain walks are not random SSD IOPS.
//!
//! **Restart safety:** deletes incomplete `scripthash.migrate/`, restores v3 from
//! `scripthash.v3.bak/` if the primary was mid-swap, then rebuilds cleanly.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::scripthash::ScriptHashBulkBuilder;
use crate::scripthash_layout::{ShEntry, SH_ALLOC_MAGIC, SH_V3_RECORD_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use rbitcoin_primitives::{Fk, SCHEMA_VERSION, STORE_MAGIC, TableKind};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
use std::time::Instant;

const PREV_SCHEMA: u16 = 3;
/// Load entire v3 body into RAM when ≤ this size (chain walks become DRAM).
const BODY_RAM_LOAD_MAX: u64 = 12 * 1024 * 1024 * 1024; // 12 GiB
const PROGRESS_EVERY_KEYS: u64 = 100_000;

/// Migrate durable scripthash head/body from v3 linked lists to hybrid slabs.
///
/// Safe to call when `scripthash.runs/` is non-empty (runs are not modified).
/// Idempotent when body already has SHAL magic.
///
/// **Partial runs:** you do **not** need to manually delete anything. This always
/// clears `scripthash.migrate/` and repairs a mid-swap from `scripthash.v3.bak/`
/// before starting a fresh bulk convert.
pub fn migrate_scripthash(store_dir: &Path) -> Result<(), StoreError> {
    recover_partial_state(store_dir)?;

    let body_path = store_dir.join("scripthash.body");
    if !body_path.exists() {
        // Empty / missing SH: still stamp meta if needed.
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        return Ok(());
    }
    if body_has_shal(&body_path)? {
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        cleanup_side_dirs(store_dir);
        return Ok(());
    }

    rbitcoin_log::info!(
        "store: migrating scripthash to hybrid slabs (bulk) path={}",
        store_dir.display()
    );
    let t0 = Instant::now();

    let work = store_dir.join("scripthash.migrate");
    // recover_partial_state already removed work; create fresh.
    if work.exists() {
        std::fs::remove_dir_all(&work).map_err(|e| StoreError::io(&work, e))?;
    }
    std::fs::create_dir_all(&work).map_err(|e| StoreError::io(&work, e))?;

    let old_body = TableFile::open_with_schema(&body_path, TableKind::ScriptHash, PREV_SCHEMA)?;
    let old_head = open_v3_head(store_dir)?;

    let body_payload = old_body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
    if body_payload % SH_V3_RECORD_LEN as u64 != 0 {
        return Err(StoreError::Corrupt(
            "v3 scripthash body size not multiple of 20",
        ));
    }
    let row_count = body_payload / SH_V3_RECORD_LEN as u64;
    let expected_keys = old_head.occupied();

    rbitcoin_log::info!(
        "store: scripthash migrate scan v3 body_rows={} head_occupied={} body_bytes={}",
        row_count,
        expected_keys,
        body_payload
    );

    // Prefer RAM-resident body so chain walks are not random device IOPS.
    let body_bytes: Option<Vec<u8>> = if body_payload > 0 && body_payload <= BODY_RAM_LOAD_MAX {
        let t_load = Instant::now();
        let mut buf = vec![0u8; body_payload as usize];
        old_body.read_at(FILE_HEADER_LEN as u64, &mut buf)?;
        rbitcoin_log::info!(
            "store: scripthash migrate loaded v3 body into RAM ({:.2} GiB) in {:?}",
            body_payload as f64 / (1024.0 * 1024.0 * 1024.0),
            t_load.elapsed()
        );
        Some(buf)
    } else {
        if body_payload > BODY_RAM_LOAD_MAX {
            rbitcoin_log::warn!(
                "store: scripthash migrate body too large for RAM load ({:.2} GiB); using random reads",
                body_payload as f64 / (1024.0 * 1024.0 * 1024.0)
            );
        }
        None
    };

    let mut builder = ScriptHashBulkBuilder::create(&work, expected_keys)?;
    let mut live_total = 0u64;
    let mut keys_done = 0u64;
    let mut keys_skipped_empty = 0u64;
    let mut chain_buf: Vec<ShEntry> = Vec::with_capacity(16);
    let mut row_tmp = [0u8; SH_V3_RECORD_LEN];

    for_each_v3_head_fk(&old_head, |key, head_fk| {
        chain_buf.clear();
        let mut cur = Some(head_fk);
        while let Some(fk) = cur {
            let id = match fk.get() {
                Some(i) if i > 0 && i <= row_count => i,
                _ => break,
            };
            let row = read_v3_row(&old_body, body_bytes.as_deref(), id, &mut row_tmp)?;
            let create_tx_fk = Fk(u64::from_le_bytes(row[0..8].try_into().unwrap()));
            let vout = u32::from_le_bytes(row[8..12].try_into().unwrap());
            let next = Fk(u64::from_le_bytes(row[12..20].try_into().unwrap()));
            if !create_tx_fk.is_null() {
                chain_buf.push(ShEntry::new(create_tx_fk, vout));
            }
            cur = if next.is_null() { None } else { Some(next) };
        }
        // v3 newest-first → oldest-first for hybrid append order.
        chain_buf.reverse();
        if chain_buf.is_empty() {
            keys_skipped_empty += 1;
            return Ok(());
        }
        live_total += chain_buf.len() as u64;
        builder.put_chain(key, &chain_buf)?;
        keys_done += 1;
        if keys_done % PROGRESS_EVERY_KEYS == 0 {
            rbitcoin_log::info!(
                "store: scripthash migrate progress keys={} live_creates={} elapsed={:?}",
                keys_done,
                live_total,
                t0.elapsed()
            );
        }
        Ok(())
    })?;

    let (live_written, head_keys) = builder.finish()?;
    drop(old_body);
    drop(old_head);
    drop(body_bytes);

    rbitcoin_log::info!(
        "store: scripthash migrate packed keys={} (empty_chains={}) live={} head_slots={} build={:?}",
        keys_done,
        keys_skipped_empty,
        live_written,
        head_keys,
        t0.elapsed()
    );

    // Install: backup v3 → move hybrid into place → stamp → drop backup.
    let backup = store_dir.join("scripthash.v3.bak");
    if backup.exists() {
        std::fs::remove_dir_all(&backup).map_err(|e| StoreError::io(&backup, e))?;
    }
    std::fs::create_dir_all(&backup).map_err(|e| StoreError::io(&backup, e))?;

    let old_head_path = store_dir.join("scripthash.head");
    move_path(&body_path, &backup.join("scripthash.body"))?;
    move_path(&old_head_path, &backup.join("scripthash.head"))?;
    move_path(&work.join("scripthash.body"), &body_path)?;
    move_path(&work.join("scripthash.head"), &old_head_path)?;
    let _ = std::fs::remove_dir_all(&work);

    stamp_store_schema(store_dir, SCHEMA_VERSION)?;
    let _ = std::fs::remove_dir_all(&backup);

    rbitcoin_log::info!(
        "store: scripthash migrate done keys={} live_creates={} total={:?}",
        keys_done,
        live_written,
        t0.elapsed()
    );
    Ok(())
}

/// Repair incomplete previous attempts so a fresh migrate can run.
///
/// - Hybrid body already in place → stamp + drop side dirs.
/// - Mid-swap (primary missing pieces, bak present) → restore v3 from bak.
/// - Always drop incomplete `scripthash.migrate/`.
fn recover_partial_state(store_dir: &Path) -> Result<(), StoreError> {
    let body_path = store_dir.join("scripthash.body");
    let head_path = store_dir.join("scripthash.head");
    let work = store_dir.join("scripthash.migrate");
    let backup = store_dir.join("scripthash.v3.bak");

    // Incomplete build workspace: always discard (we rebuild).
    if work.exists() {
        rbitcoin_log::info!(
            "store: scripthash migrate removing incomplete work dir {}",
            work.display()
        );
        std::fs::remove_dir_all(&work).map_err(|e| StoreError::io(&work, e))?;
    }

    let primary_shal = body_path.exists() && body_has_shal(&body_path)?;
    if primary_shal {
        // Finished or almost finished install.
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        if backup.exists() {
            rbitcoin_log::info!(
                "store: scripthash migrate dropping leftover backup {}",
                backup.display()
            );
            let _ = std::fs::remove_dir_all(&backup);
        }
        return Ok(());
    }

    // Mid-swap: v3 lives only under bak (primary body/head moved away).
    if backup.exists() {
        let bak_body = backup.join("scripthash.body");
        let bak_head = backup.join("scripthash.head");
        let need_restore = !body_path.exists()
            || !head_path.exists()
            || (body_path.exists() && !is_v3_body(&body_path)? && !body_has_shal(&body_path)?);

        if need_restore && bak_body.exists() {
            rbitcoin_log::warn!(
                "store: scripthash migrate restoring v3 primary from {}",
                backup.display()
            );
            if body_path.exists() {
                let _ = std::fs::remove_file(&body_path);
            }
            if head_path.exists() {
                let _ = if head_path.is_dir() {
                    std::fs::remove_dir_all(&head_path)
                } else {
                    std::fs::remove_file(&head_path)
                };
            }
            move_path(&bak_body, &body_path)?;
            if bak_head.exists() {
                move_path(&bak_head, &head_path)?;
            }
        }

        // Stale bak next to healthy v3 primary — drop it.
        if body_path.exists() && is_v3_body(&body_path)? {
            let _ = std::fs::remove_dir_all(&backup);
        }
    }

    Ok(())
}

fn cleanup_side_dirs(store_dir: &Path) {
    let work = store_dir.join("scripthash.migrate");
    let backup = store_dir.join("scripthash.v3.bak");
    if work.exists() {
        let _ = std::fs::remove_dir_all(&work);
    }
    if backup.exists() {
        let _ = std::fs::remove_dir_all(&backup);
    }
}

fn is_v3_body(path: &Path) -> Result<bool, StoreError> {
    if body_has_shal(path)? {
        return Ok(false);
    }
    // Heuristic: logical payload multiple of 20 via file size (best-effort without full open).
    let meta = std::fs::metadata(path).map_err(|e| StoreError::io(path, e))?;
    let len = meta.len();
    if len < FILE_HEADER_LEN as u64 {
        return Ok(false);
    }
    Ok((len - FILE_HEADER_LEN as u64) % SH_V3_RECORD_LEN as u64 == 0)
}

fn read_v3_row<'a>(
    body: &TableFile,
    ram: Option<&'a [u8]>,
    id: u64,
    tmp: &'a mut [u8; SH_V3_RECORD_LEN],
) -> Result<&'a [u8; SH_V3_RECORD_LEN], StoreError> {
    let off = (id - 1) * SH_V3_RECORD_LEN as u64;
    if let Some(bytes) = ram {
        let start = off as usize;
        let end = start + SH_V3_RECORD_LEN;
        if end > bytes.len() {
            return Err(StoreError::Corrupt("v3 scripthash row OOB"));
        }
        tmp.copy_from_slice(&bytes[start..end]);
        return Ok(tmp);
    }
    let file_off = FILE_HEADER_LEN as u64 + off;
    body.read_at(file_off, tmp)?;
    Ok(tmp)
}

fn body_has_shal(path: &Path) -> Result<bool, StoreError> {
    let mut f = std::fs::File::open(path).map_err(|e| StoreError::io(path, e))?;
    f.seek(SeekFrom::Start(FILE_HEADER_LEN as u64))
        .map_err(|e| StoreError::io(path, e))?;
    let mut magic = [0u8; 4];
    match f.read_exact(&mut magic) {
        Ok(()) => Ok(magic == SH_ALLOC_MAGIC),
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(e) => Err(StoreError::io(path, e)),
    }
}

fn open_v3_head(store_dir: &Path) -> Result<ShardedHashHead, StoreError> {
    let path = store_dir.join("scripthash.head");
    open_sharded_hash_head_schema(&path, PREV_SCHEMA)
}

fn open_sharded_hash_head_schema(path: &Path, schema: u16) -> Result<ShardedHashHead, StoreError> {
    if path.is_dir() || path.is_file() {
        return ShardedHashHead::open_with_schema(path, schema);
    }
    Err(StoreError::io(
        path,
        std::io::Error::new(std::io::ErrorKind::NotFound, "v3 head missing"),
    ))
}

fn for_each_v3_head_fk(
    head: &ShardedHashHead,
    f: impl FnMut([u8; 32], Fk) -> Result<(), StoreError>,
) -> Result<(), StoreError> {
    head.for_each_occupied_fk(f)
}

fn move_path(from: &Path, to: &Path) -> Result<(), StoreError> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| StoreError::io(parent, e))?;
    }
    std::fs::rename(from, to).map_err(|e| StoreError::io(from, e))
}

/// Stamp meta + every RBT1 table header under `store_dir` to `schema`.
pub fn stamp_store_schema(store_dir: &Path, schema: u16) -> Result<(), StoreError> {
    let meta = store_dir.join("meta");
    if meta.exists() {
        let mut bytes = std::fs::read(&meta).map_err(|e| StoreError::io(&meta, e))?;
        if bytes.len() >= 6 && bytes[0..4] == STORE_MAGIC {
            bytes[4..6].copy_from_slice(&schema.to_le_bytes());
            std::fs::write(&meta, &bytes).map_err(|e| StoreError::io(&meta, e))?;
        }
    }
    let epoch = store_dir.join("archive_epoch");
    if epoch.exists() {
        let _ = TableFile::stamp_schema_on_path(&epoch, schema);
    }
    stamp_tree(store_dir, schema)?;
    Ok(())
}

fn stamp_tree(dir: &Path, schema: u16) -> Result<(), StoreError> {
    let rd = std::fs::read_dir(dir).map_err(|e| StoreError::io(dir, e))?;
    for ent in rd.flatten() {
        let p = ent.path();
        let name = ent.file_name().to_string_lossy().into_owned();
        if name.ends_with(".runs")
            || name.ends_with(".bak")
            || name == "scripthash.migrate"
            || name == "scripthash.v3.bak"
        {
            continue;
        }
        if p.is_dir() {
            stamp_tree(&p, schema)?;
            continue;
        }
        if let Ok(mut f) = std::fs::File::open(&p) {
            let mut magic = [0u8; 4];
            if f.read_exact(&mut magic).is_ok() && magic == STORE_MAGIC {
                let _ = TableFile::stamp_schema_on_path(&p, schema);
            }
        }
    }
    Ok(())
}

/// Ensure store is schema 4 with hybrid SH (migrate if needed). Call before `Store::open` tables.
pub fn ensure_scripthash_hybrid(store_dir: &Path) -> Result<(), StoreError> {
    let meta = store_dir.join("meta");
    if !meta.exists() {
        return Ok(());
    }
    let bytes = std::fs::read(&meta).map_err(|e| StoreError::io(&meta, e))?;
    if bytes.len() < 6 || bytes[0..4] != STORE_MAGIC {
        return Err(StoreError::BadMagic);
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver == SCHEMA_VERSION {
        let body = store_dir.join("scripthash.body");
        if body.exists() && !body_has_shal(&body)? {
            migrate_scripthash(store_dir)?;
        } else {
            // Drop any leftover side dirs from a prior success.
            recover_partial_state(store_dir)?;
        }
        return Ok(());
    }
    if ver == PREV_SCHEMA {
        migrate_scripthash(store_dir)?;
        return Ok(());
    }
    let body = store_dir.join("scripthash.body");
    if body.exists() && body_has_shal(&body)? {
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        cleanup_side_dirs(store_dir);
        return Ok(());
    }
    // Try recover then re-check.
    recover_partial_state(store_dir)?;
    let bytes = std::fs::read(&meta).map_err(|e| StoreError::io(&meta, e))?;
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver == PREV_SCHEMA || (store_dir.join("scripthash.body").exists()
        && !body_has_shal(&store_dir.join("scripthash.body"))?)
    {
        migrate_scripthash(store_dir)?;
        return Ok(());
    }
    Err(StoreError::BadSchema(ver))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FILE_HEADER_LEN;
    use crate::hashhead::HashHead;
    use crate::scripthash::ScriptHashTable;
    use rbitcoin_primitives::TableKind;
    use std::io::Write;
    use std::path::PathBuf;

    fn tmp() -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "rbitcoin-sh-mig-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn write_v3_fixture(dir: &Path, keys_and_chains: &[([u8; 32], &[(u64, u32)])]) {
        {
            let mut f = std::fs::File::create(dir.join("meta")).unwrap();
            f.write_all(&STORE_MAGIC).unwrap();
            f.write_all(&3u16.to_le_bytes()).unwrap();
        }
        let body = TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
        drop(body);
        TableFile::stamp_schema_on_path(dir.join("scripthash.body"), 3).unwrap();
        let body =
            TableFile::open_with_schema(dir.join("scripthash.body"), TableKind::ScriptHash, 3)
                .unwrap();
        let h = HashHead::create_with_slots(dir.join("scripthash.head"), 256).unwrap();

        let mut next_id = 1u64;
        for (key, chain) in keys_and_chains {
            // v3 is newest-first: head → newest → … → oldest → 0.
            // `chain` is oldest→newest; write oldest first with next=prev so head ends newest.
            let mut prev = 0u64;
            for &(tx, vout) in chain.iter() {
                let mut row = [0u8; SH_V3_RECORD_LEN];
                row[0..8].copy_from_slice(&tx.to_le_bytes());
                row[8..12].copy_from_slice(&vout.to_le_bytes());
                row[12..20].copy_from_slice(&prev.to_le_bytes());
                let off = FILE_HEADER_LEN as u64 + (next_id - 1) * SH_V3_RECORD_LEN as u64;
                body.write_at(off, &row).unwrap();
                prev = next_id;
                next_id += 1;
            }
            h.insert(key, Fk(prev)).unwrap(); // newest
        }
        body.flush().unwrap();
        h.flush().unwrap();
        TableFile::stamp_schema_on_path(dir.join("scripthash.head"), 3).unwrap();
    }

    #[test]
    fn migrate_v3_linked_list_to_hybrid() {
        let dir = tmp();
        let mut key = [0u8; 32];
        key[0] = 0xab;
        write_v3_fixture(&dir, &[(key, &[(5, 0)])]);

        migrate_scripthash(&dir).unwrap();
        assert!(body_has_shal(&dir.join("scripthash.body")).unwrap());

        let t = ScriptHashTable::open(&dir).unwrap();
        let ents = t.entries(&key).unwrap();
        assert_eq!(ents.len(), 1);
        assert_eq!(ents[0].1.create_tx_fk, Fk(5));
        assert_eq!(t.entry_count(), 1);

        migrate_scripthash(&dir).unwrap(); // idempotent
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn migrate_multi_create_and_cleanup_work_dir() {
        let dir = tmp();
        let mut k1 = [0u8; 32];
        k1[0] = 1;
        let mut k2 = [0u8; 32];
        k2[0] = 2;
        // oldest→newest in fixture helper
        write_v3_fixture(
            &dir,
            &[
                (k1, &[(10, 0), (11, 1), (12, 2), (13, 3), (14, 4)]), // 5 → slab class1
                (k2, &[(20, 0)]),
            ],
        );

        // Simulate leftover work dir from a killed migrate.
        std::fs::create_dir_all(dir.join("scripthash.migrate/junk")).unwrap();
        std::fs::write(dir.join("scripthash.migrate/junk/x"), b"nope").unwrap();

        migrate_scripthash(&dir).unwrap();
        assert!(!dir.join("scripthash.migrate").exists());
        assert!(!dir.join("scripthash.v3.bak").exists());

        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&k1).unwrap().len(), 5);
        assert_eq!(t.entries(&k2).unwrap().len(), 1);
        assert_eq!(t.entry_count(), 6);
        // Order oldest→newest
        let e1 = t.entries(&k1).unwrap();
        assert_eq!(e1[0].1.create_tx_fk, Fk(10));
        assert_eq!(e1[4].1.create_tx_fk, Fk(14));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recover_restores_v3_from_backup() {
        let dir = tmp();
        let mut key = [0u8; 32];
        key[0] = 0xcd;
        write_v3_fixture(&dir, &[(key, &[(7, 1)])]);

        // Fake mid-swap: move primary into bak, leave no primary.
        let bak = dir.join("scripthash.v3.bak");
        std::fs::create_dir_all(&bak).unwrap();
        std::fs::rename(dir.join("scripthash.body"), bak.join("scripthash.body")).unwrap();
        std::fs::rename(dir.join("scripthash.head"), bak.join("scripthash.head")).unwrap();
        assert!(!dir.join("scripthash.body").exists());

        migrate_scripthash(&dir).unwrap();
        assert!(body_has_shal(&dir.join("scripthash.body")).unwrap());
        let t = ScriptHashTable::open(&dir).unwrap();
        assert_eq!(t.entries(&key).unwrap()[0].1.create_tx_fk, Fk(7));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
