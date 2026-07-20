//! One-time migration: v3 linked-list scripthash → hybrid inline/slab (schema 4).
//!
//! Touches only `scripthash.head` + `scripthash.body`. `scripthash.runs/` is left
//! untouched; materialize before or after migrate uses the live table format.

use crate::error::StoreError;
use crate::file::{TableFile, FILE_HEADER_LEN};
use crate::scripthash::ScriptHashTable;
use crate::scripthash_layout::{ShEntry, SH_ALLOC_MAGIC, SH_V3_RECORD_LEN};
use crate::sharded_hashhead::ShardedHashHead;
use rbitcoin_primitives::{Fk, SCHEMA_VERSION, STORE_MAGIC, TableKind};
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;
#[cfg(test)]
use std::path::PathBuf;

const PREV_SCHEMA: u16 = 3;

/// Migrate durable scripthash head/body from v3 linked lists to hybrid slabs.
///
/// Safe to call when `scripthash.runs/` is non-empty (runs are not modified).
/// Idempotent when body already has SHAL magic.
pub fn migrate_scripthash(store_dir: &Path) -> Result<(), StoreError> {
    let body_path = store_dir.join("scripthash.body");
    if !body_path.exists() {
        return Ok(());
    }
    if body_has_shal(&body_path)? {
        // Already hybrid; ensure meta/headers stamped if needed.
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        return Ok(());
    }

    rbitcoin_log::info!(
        "store: migrating scripthash to hybrid slabs path={}",
        store_dir.display()
    );

    let work = store_dir.join("scripthash.migrate");
    if work.exists() {
        std::fs::remove_dir_all(&work).map_err(|e| StoreError::io(&work, e))?;
    }
    std::fs::create_dir_all(&work).map_err(|e| StoreError::io(&work, e))?;

    // Open v3 tables (40 B head slots, 20 B body rows).
    let old_body = TableFile::open_with_schema(&body_path, TableKind::ScriptHash, PREV_SCHEMA)?;
    let old_head = open_v3_head(store_dir)?;

    let body_len = old_body.logical_len().saturating_sub(FILE_HEADER_LEN as u64);
    if body_len % SH_V3_RECORD_LEN as u64 != 0 {
        return Err(StoreError::Corrupt(
            "v3 scripthash body size not multiple of 20",
        ));
    }
    let row_count = body_len / SH_V3_RECORD_LEN as u64;

    // Create fresh hybrid table under work/.
    let new_table = ScriptHashTable::create(&work)?;
    // We will build via internal head/body: use put_create_batch on synthetic records.
    // Faster path: walk old head, pack directly.

    let mut live_total = 0u64;
    let mut keys_done = 0u64;

    // Collect all (key, head_fk) from v3 head.
    for_each_v3_head_fk(&old_head, |key, head_fk| {
        let mut chain = Vec::new();
        let mut cur = Some(head_fk);
        while let Some(fk) = cur {
            let id = fk.get().ok_or(StoreError::InvalidFk)?;
            if id == 0 || id > row_count {
                break;
            }
            let offset = FILE_HEADER_LEN as u64 + (id - 1) * SH_V3_RECORD_LEN as u64;
            let mut buf = [0u8; SH_V3_RECORD_LEN];
            old_body.read_at(offset, &mut buf)?;
            let create_tx_fk = Fk(u64::from_le_bytes(buf[0..8].try_into().unwrap()));
            let vout = u32::from_le_bytes(buf[8..12].try_into().unwrap());
            let next = Fk(u64::from_le_bytes(buf[12..20].try_into().unwrap()));
            if !create_tx_fk.is_null() {
                chain.push(ShEntry::new(create_tx_fk, vout));
            }
            cur = if next.is_null() { None } else { Some(next) };
        }
        // v3 is newest-first; reverse to oldest-first.
        chain.reverse();
        if chain.is_empty() {
            return Ok(());
        }

        // Install into new table via public put API.
        let recs: Vec<_> = chain
            .iter()
            .map(|e| crate::scripthash::ScriptHashRecord {
                scripthash: key,
                create_tx_fk: e.create_tx_fk,
                vout: e.vout,
                next: Fk::NULL,
                txid: [0u8; 32],
                value: 0,
                create_height: 0,
            })
            .collect();
        new_table.put_create_batch(&recs)?;
        live_total += chain.len() as u64;
        keys_done += 1;
        if keys_done % 50_000 == 0 {
            rbitcoin_log::info!(
                "store: scripthash migrate progress keys={} live_creates={}",
                keys_done,
                live_total
            );
        }
        Ok(())
    })?;

    new_table.flush()?;
    drop(new_table);
    drop(old_body);
    drop(old_head);

    // Install: backup old → move new into place.
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

    // Drop backup after successful stamp.
    let _ = std::fs::remove_dir_all(&backup);

    rbitcoin_log::info!(
        "store: scripthash migrate done keys≈{} live_creates={}",
        keys_done,
        live_total
    );
    Ok(())
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
    // ShardedHashHead uses HashHead::open → SCHEMA_VERSION. Temporarily open
    // shards with schema 3 via a local helper.
    open_sharded_hash_head_schema(&path, PREV_SCHEMA)
}

fn open_sharded_hash_head_schema(path: &Path, schema: u16) -> Result<ShardedHashHead, StoreError> {
    // Reuse create_sharded structure by opening each file with schema.
    // ShardedHashHead does not expose open_with_schema — open via HashHead internals.
    // For v3 single-file or dir:
    if path.is_dir() {
        let mut names: Vec<String> = std::fs::read_dir(path)
            .map_err(|e| StoreError::io(path, e))?
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().map(|t| t.is_file()).unwrap_or(false))
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        if names.is_empty() {
            return Err(StoreError::Corrupt("empty v3 scripthash head dir"));
        }
        // Build via HashHead::open_with_schema if we add it; else TableFile + reconstruct.
        // Use public open after temporarily... we add HashHead::open_with_schema below.
        return ShardedHashHead::open_with_schema(path, schema);
    }
    if path.is_file() {
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
    // meta
    let meta = store_dir.join("meta");
    if meta.exists() {
        let mut bytes = std::fs::read(&meta).map_err(|e| StoreError::io(&meta, e))?;
        if bytes.len() >= 6 && bytes[0..4] == STORE_MAGIC {
            bytes[4..6].copy_from_slice(&schema.to_le_bytes());
            std::fs::write(&meta, &bytes).map_err(|e| StoreError::io(&meta, e))?;
        }
    }
    // archive_epoch
    let epoch = store_dir.join("archive_epoch");
    if epoch.exists() {
        let _ = TableFile::stamp_schema_on_path(&epoch, schema);
    }

    // Walk store_dir for files (not dirs like *.runs, *.head shards).
    stamp_tree(store_dir, schema)?;
    Ok(())
}

fn stamp_tree(dir: &Path, schema: u16) -> Result<(), StoreError> {
    let rd = std::fs::read_dir(dir).map_err(|e| StoreError::io(dir, e))?;
    for ent in rd.flatten() {
        let p = ent.path();
        let name = ent.file_name().to_string_lossy().into_owned();
        if name.ends_with(".runs") || name.ends_with(".bak") || name == "scripthash.migrate" {
            continue;
        }
        if p.is_dir() {
            // head shard dirs, etc.
            stamp_tree(&p, schema)?;
            continue;
        }
        // Only stamp RBT1 files.
        if let Ok(mut f) = std::fs::File::open(&p) {
            let mut magic = [0u8; 4];
            if f.read_exact(&mut magic).is_ok() && magic == STORE_MAGIC {
                let _ = TableFile::stamp_schema_on_path(&p, schema);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::FILE_HEADER_LEN;
    use crate::hashhead::HashHead;
    use rbitcoin_primitives::TableKind;
    use std::io::Write;

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

    /// Hand-build a minimal v3 SH (schema 3 headers) and migrate to hybrid.
    #[test]
    fn migrate_v3_linked_list_to_hybrid() {
        use std::path::PathBuf;
        let dir = tmp();
        // meta schema 3
        {
            let mut f = std::fs::File::create(dir.join("meta")).unwrap();
            f.write_all(&STORE_MAGIC).unwrap();
            f.write_all(&3u16.to_le_bytes()).unwrap();
        }
        // v3 body: one 20 B row
        {
            let body = TableFile::create(dir.join("scripthash.body"), TableKind::ScriptHash).unwrap();
            // Stamp schema back to 3
            drop(body);
            TableFile::stamp_schema_on_path(dir.join("scripthash.body"), 3).unwrap();
            let body = TableFile::open_with_schema(
                dir.join("scripthash.body"),
                TableKind::ScriptHash,
                3,
            )
            .unwrap();
            let mut row = [0u8; SH_V3_RECORD_LEN];
            row[0..8].copy_from_slice(&5u64.to_le_bytes()); // create_tx_fk
            row[8..12].copy_from_slice(&0u32.to_le_bytes()); // vout
            // next = 0
            body.write_at(FILE_HEADER_LEN as u64, &row).unwrap();
            body.flush().unwrap();
        }
        // v3 head: key → fk 1
        {
            let mut key = [0u8; 32];
            key[0] = 0xab;
            let h = HashHead::create_with_slots(dir.join("scripthash.head"), 64).unwrap();
            h.insert(&key, Fk(1)).unwrap();
            h.flush().unwrap();
            TableFile::stamp_schema_on_path(dir.join("scripthash.head"), 3).unwrap();
        }

        migrate_scripthash(&dir).unwrap();
        assert!(body_has_shal(&dir.join("scripthash.body")).unwrap());

        let t = ScriptHashTable::open(&dir).unwrap();
        let mut key = [0u8; 32];
        key[0] = 0xab;
        let ents = t.entries(&key).unwrap();
        assert_eq!(ents.len(), 1);
        assert_eq!(ents[0].1.create_tx_fk, Fk(5));
        assert_eq!(t.entry_count(), 1);

        // Idempotent second migrate
        migrate_scripthash(&dir).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        let _ = PathBuf::new();
    }
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
        // Still verify SH hybrid if body exists.
        let body = store_dir.join("scripthash.body");
        if body.exists() && !body_has_shal(&body)? {
            migrate_scripthash(store_dir)?;
        }
        return Ok(());
    }
    if ver == PREV_SCHEMA {
        migrate_scripthash(store_dir)?;
        return Ok(());
    }
    // Hybrid body already installed but meta not stamped (crash mid-migrate).
    let body = store_dir.join("scripthash.body");
    if body.exists() && body_has_shal(&body)? {
        stamp_store_schema(store_dir, SCHEMA_VERSION)?;
        return Ok(());
    }
    Err(StoreError::BadSchema(ver))
}


