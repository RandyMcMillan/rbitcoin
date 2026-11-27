use bitcoin::hashes::Hash;
use bitcoin::Txid;
use rbitcoin_mempool::error::MempoolError;
use rbitcoin_mempool::packed::{VinAux, SIGOP_COST_OFF};
use rbitcoin_mempool::store::Mempool;
use rbitcoin_mempool::store::*;
use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

/// Rewrite a closed current pool under `dir` as schema 2 (drop each
/// record's sigops and accept time, stamp headers 2) — pre-upgrade fixture.
pub(crate) fn downgrade_to_schema2(dir: &Path) {
    downgrade_to(dir, 2);
}

/// Rewrite a closed current pool under `dir` as schema 3 (drop each
/// record's accept time, stamp headers 3) — pre-upgrade fixture.
pub(crate) fn downgrade_to_schema3(dir: &Path) {
    downgrade_to(dir, 3);
}

fn downgrade_to(dir: &Path, schema: u16) {
    let mut slots = fs::read(dir.join("slots")).unwrap();
    let body = fs::read(dir.join("tx.body")).unwrap();
    let cap = u32::from_le_bytes(slots[8..12].try_into().unwrap());
    let mut new_body = body[..BODY_HEADER].to_vec();
    for slot in 0..cap as usize {
        let off = SLOTS_HEADER + slot * SLOT_REC;
        if slots[off] != SLOT_LIVE {
            continue;
        }
        let b_off = u64::from_le_bytes(slots[off + 4..off + 12].try_into().unwrap()) as usize;
        let b_len = u32::from_le_bytes(slots[off + 12..off + 16].try_into().unwrap()) as usize;
        let mut rec = body[b_off..b_off + b_len].to_vec();
        // Accept time sits right after sigops; schema 2 drops both.
        let from = if schema == 2 {
            SIGOP_COST_OFF
        } else {
            SIGOP_COST_OFF + 8
        };
        rec.drain(from..SIGOP_COST_OFF + 16);
        slots[off + 4..off + 12].copy_from_slice(&(new_body.len() as u64).to_le_bytes());
        slots[off + 12..off + 16].copy_from_slice(&(rec.len() as u32).to_le_bytes());
        new_body.extend_from_slice(&rec);
    }
    let len = new_body.len() as u64;
    new_body[8..16].copy_from_slice(&len.to_le_bytes());
    new_body[4..6].copy_from_slice(&schema.to_le_bytes());
    slots[4..6].copy_from_slice(&schema.to_le_bytes());
    let mut meta = fs::read(dir.join("meta")).unwrap();
    meta[4..6].copy_from_slice(&schema.to_le_bytes());
    fs::write(dir.join("tx.body"), new_body).unwrap();
    fs::write(dir.join("slots"), slots).unwrap();
    fs::write(dir.join("meta"), meta).unwrap();
}

#[test]
fn schema2_converts_on_open_with_unknown_sigops() {
    let dir = tmp_dir();
    let tx = tiny_tx();
    let tid = tx.compute_txid();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let sh = [0x11u8; 32];
        let aux = [VinAux {
            prev_txid: tx.input[0].previous_output.txid,
            vout: tx.input[0].previous_output.vout,
            script_hash: Some(sh),
            create_fk: None,
        }];
        mp.append_live_tx(
            &tx,
            &tid,
            &tx.compute_wtxid(),
            55,
            400,
            12,
            1_700_000_000,
            &aux,
        )
        .unwrap();
        mp.flush().unwrap();
    }
    downgrade_to_schema2(&dir);
    let mut mp = Mempool::open_or_create(&dir).expect("schema 2 converts");
    for f in ["meta", "slots", "tx.body"] {
        let b = fs::read(dir.join(f)).unwrap();
        assert_eq!(&b[4..6], &MEM_SCHEMA.to_le_bytes(), "{f} stamped");
    }
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live.len(), 1);
    let p = &live[0].packed;
    assert_eq!((p.fee_sat, p.weight, p.txid), (55, 400, tid));
    assert_eq!(p.sigop_cost, None, "schema 2 has no cost");
    assert_eq!(p.vins[0].script_hash, Some([0x11u8; 32]), "vin aux kept");

    mp.set_sigop_cost(live[0].slot, 12).unwrap();
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.load_live_txs().unwrap()[0].packed.sigop_cost, Some(12));
}

/// Move the one live record of a closed pool so it starts at body offset `to`.
fn move_only_record(dir: &Path, to: usize) {
    let mut slots = fs::read(dir.join("slots")).unwrap();
    let body = fs::read(dir.join("tx.body")).unwrap();
    let off = SLOTS_HEADER;
    assert_eq!(slots[off], SLOT_LIVE);
    let b_off = u64::from_le_bytes(slots[off + 4..off + 12].try_into().unwrap()) as usize;
    let b_len = u32::from_le_bytes(slots[off + 12..off + 16].try_into().unwrap()) as usize;
    assert!(to >= b_off);
    let mut new_body = body[..b_off].to_vec();
    new_body.resize(to, 0);
    new_body.extend_from_slice(&body[b_off..b_off + b_len]);
    let len = new_body.len() as u64;
    new_body[8..16].copy_from_slice(&len.to_le_bytes());
    slots[off + 4..off + 12].copy_from_slice(&(to as u64).to_le_bytes());
    fs::write(dir.join("tx.body"), new_body).unwrap();
    fs::write(dir.join("slots"), slots).unwrap();
}

/// A schema-2 pool whose one LIVE record has its sigop cost crossing the
/// sector boundary at 512, unknown on disk (0xFF). Returns the txid.
fn cross_sector_sigop_pool(dir: &Path) -> Txid {
    let tx = tiny_tx();
    let tid = tx.compute_txid();
    {
        let mut mp = Mempool::open_or_create(dir).unwrap();
        mp.append_live_tx(
            &tx,
            &tid,
            &tx.compute_wtxid(),
            55,
            400,
            12,
            1_700_000_000,
            &[],
        )
        .unwrap();
        mp.flush().unwrap();
    }
    downgrade_to_schema2(dir);
    let record = 510 - SIGOP_COST_OFF;
    let mp = Mempool::open_or_create(dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live[0].packed.sigop_cost, None);
    drop(mp);
    move_only_record(dir, record);
    tid
}

/// A recomputed cost whose 8 bytes cross a 512-byte sector is written low
/// sector first, synced, then high sector. The cost is durable, and the one
/// crash state between the two writes decodes as unknown.
#[test]
fn a_sigop_cost_across_a_sector_is_written_low_sector_first() {
    let dir = tmp_dir();
    let tid = cross_sector_sigop_pool(&dir);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live[0].packed.txid, tid);
    mp.set_sigop_cost(live[0].slot, 0x0400).unwrap();
    assert_eq!(
        mp.load_live_txs().unwrap()[0].packed.sigop_cost,
        Some(0x0400)
    );
    drop(mp);
    let body = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(&body[510..518], &0x0400u64.to_le_bytes());
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(
        mp.load_live_txs().unwrap()[0].packed.sigop_cost,
        Some(0x0400)
    );
    drop(mp);
    // A crash after the low sector: new low bytes under the old 0xFF high
    // bytes decode above the block limit, so the cost reads unknown.
    let mut file = OpenOptions::new()
        .write(true)
        .open(dir.join("tx.body"))
        .unwrap();
    file.seek(SeekFrom::Start(512)).unwrap();
    file.write_all(&[0xFFu8; 6]).unwrap();
    drop(file);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.load_live_txs().unwrap()[0].packed.sigop_cost, None);
}

/// The sync between the two sector writes is the barrier. When it fails,
/// the high bytes are still the old 0xFF, so the cost reads unknown, not a
/// mix of new low bytes and a new high byte.
#[cfg(debug_assertions)]
#[test]
fn a_failed_sigop_sync_leaves_the_high_sector_old() {
    let dir = tmp_dir();
    let tid = cross_sector_sigop_pool(&dir);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live[0].packed.txid, tid);
    rbitcoin_mempool::store::set_mempool_sigop_sync_fails(&dir, true);
    assert!(mp.set_sigop_cost(live[0].slot, 0x0400).is_err());
    rbitcoin_mempool::store::set_mempool_sigop_sync_fails(&dir, false);
    drop(mp);
    let body = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(&body[512..518], &[0xFFu8; 6], "the high sector was written");
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.load_live_txs().unwrap()[0].packed.sigop_cost, None);
}

/// Schema 3 records keep fee, weight, sigops and vin aux, and get the
/// migrate's wall-clock second as their accept time.
#[test]
fn schema3_converts_on_open_with_migrate_accept_time() {
    let dir = tmp_dir();
    let tx = tiny_tx();
    let tid = tx.compute_txid();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.append_live_tx(
            &tx,
            &tid,
            &tx.compute_wtxid(),
            55,
            400,
            12,
            1_700_000_000,
            &[],
        )
        .unwrap();
        mp.flush().unwrap();
    }
    downgrade_to_schema3(&dir);
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mp = Mempool::open_or_create(&dir).expect("schema 3 converts");
    for f in ["meta", "slots", "tx.body"] {
        let b = fs::read(dir.join(f)).unwrap();
        assert_eq!(&b[4..6], &MEM_SCHEMA.to_le_bytes(), "{f} stamped");
    }
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live.len(), 1);
    let p = &live[0].packed;
    assert_eq!((p.fee_sat, p.weight, p.txid), (55, 400, tid));
    assert_eq!(p.sigop_cost, Some(12), "schema 3 cost kept");
    assert!(
        p.accept_secs >= before && p.accept_secs < before + 60,
        "migrate stamps its own time: {} vs {before}",
        p.accept_secs
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(
        mp.load_live_txs().unwrap()[0].packed.accept_secs,
        p.accept_secs,
        "the stamp is durable, not redone each open"
    );
}

/// A legacy LIVE slot whose range runs past the body, or is shorter than
/// the 16-byte fee‖weight prefix, refuses open as Corrupt (no panic).
#[test]
fn schema2_bad_slot_range_is_corrupt() {
    let tx = tiny_tx();
    let tid = tx.compute_txid();
    let range_err = "legacy live slot body range";
    for (len, want) in [
        (None, range_err),
        (Some(V1_BODY_PREFIX - 1), range_err),
        (Some(V1_BODY_PREFIX), "packed live short"),
    ] {
        let dir = tmp_dir();
        {
            let mut mp = Mempool::open_or_create(&dir).unwrap();
            mp.append_live_tx(
                &tx,
                &tid,
                &tx.compute_wtxid(),
                55,
                400,
                12,
                1_700_000_000,
                &[],
            )
            .unwrap();
            mp.flush().unwrap();
        }
        downgrade_to_schema2(&dir);
        let mut slots = fs::read(dir.join("slots")).unwrap();
        let body_len = fs::metadata(dir.join("tx.body")).unwrap().len() as usize;
        let off = SLOTS_HEADER + 12;
        let len = len.unwrap_or(body_len); // body_off > 0, so off+len > logical
        slots[off..off + 4].copy_from_slice(&(len as u32).to_le_bytes());
        fs::write(dir.join("slots"), slots).unwrap();
        let r = Mempool::open_or_create(&dir).err();
        assert!(
            matches!(r, Some(MempoolError::Corrupt(m)) if m == want),
            "len {len}: {r:?}"
        );
    }
}

/// Crash after the migrate renamed body+slots but before meta (or with a
/// stale slots header): open restamps the lagging header, keeps the record.
#[test]
fn stale_meta_or_slots_schema_restamped_on_open() {
    for stale in ["meta", "slots"] {
        let dir = tmp_dir();
        let tid = Txid::from_byte_array([0x21; 32]);
        {
            let mut mp = Mempool::open_or_create(&dir).unwrap();
            put_live(&mut mp, &tid, 5, 400);
            mp.flush().unwrap();
        }
        let mut b = fs::read(dir.join(stale)).unwrap();
        b[4..6].copy_from_slice(&2u16.to_le_bytes());
        fs::write(dir.join(stale), b).unwrap();
        let mp = Mempool::open_or_create(&dir).unwrap();
        let b = fs::read(dir.join(stale)).unwrap();
        assert_eq!(&b[4..6], &MEM_SCHEMA.to_le_bytes(), "{stale} restamped");
        assert_eq!(mp.load_live_txs().unwrap()[0].packed.txid, tid);
    }
}

fn tmp_dir() -> rbitcoin_store::testutil::TempDir {
    rbitcoin_store::testutil::TempDir::labeled("mempool").unwrap()
}

/// A new `meta` is published whole: a synced rename, not a create and a write.
#[test]
fn new_meta_is_published_through_a_synced_rename() {
    let dir = tmp_dir();
    rbitcoin_store::file::arm_fsync_dir_trace();
    let _ = open_or_init_meta(&dir.join("meta")).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    assert!(
        synced.iter().any(|d| d == dir.path()),
        "mempool meta name not synced: {synced:?}"
    );
}

/// A new `slots` or `tx.body` is the durable base later marks patch, so its
/// bytes and its directory entry are synced before open returns it.
#[cfg(all(unix, debug_assertions))]
#[test]
fn new_slots_and_body_names_are_synced() {
    let dir = tmp_dir();
    rbitcoin_store::file::arm_fsync_dir_trace();
    let _ = open_or_init_slots(&dir.join("slots"), 4).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    assert!(
        synced.iter().any(|d| d == dir.path()),
        "mempool slots name not synced: {synced:?}"
    );
    rbitcoin_store::file::arm_fsync_dir_trace();
    let _ = open_or_init_body(&dir.join("tx.body")).unwrap();
    let synced = rbitcoin_store::file::take_fsynced_dirs();
    assert!(
        synced.iter().any(|d| d == dir.path()),
        "mempool tx.body name not synced: {synced:?}"
    );
}

fn tiny_tx() -> bitcoin::Transaction {
    bitcoin::Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![bitcoin::TxIn {
            previous_output: bitcoin::OutPoint::null(),
            script_sig: bitcoin::ScriptBuf::new(),
            sequence: bitcoin::Sequence::MAX,
            witness: bitcoin::Witness::new(),
        }],
        output: vec![bitcoin::TxOut {
            value: bitcoin::Amount::from_sat(1),
            script_pubkey: bitcoin::ScriptBuf::new(),
        }],
    }
}

fn put_live(mp: &mut Mempool, tid: &Txid, fee: u64, weight: u64) -> u32 {
    let tx = tiny_tx();
    mp.append_live_tx(
        &tx,
        tid,
        &tx.compute_wtxid(),
        fee,
        weight,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap()
}

#[test]
fn empty_create_reopen_flush() {
    let dir = tmp_dir();
    {
        let mut mp = Mempool::open_or_create(&dir).expect("create");
        let m = mp.meta();
        assert_eq!(m.generation, 0);
        assert_eq!(m.live_count, 0);
        assert_eq!(m.slot_cap, DEFAULT_SLOT_CAP);
        mp.flush().expect("flush");
        assert_eq!(mp.generation(), 1);
    }
    {
        let mp = Mempool::open_or_create(&dir).expect("reopen");
        assert_eq!(mp.generation(), 1);
        assert_eq!(mp.live_count(), 0);
        assert!(mp.dir().join("meta").exists());
        assert!(mp.dir().join("slots").exists());
        assert!(mp.dir().join("tx.body").exists());
    }
    assert_eq!(
        fs::metadata(dir.join("tx.body")).unwrap().len(),
        BODY_HEADER as u64,
        "an empty body file is the 16-byte header"
    );
    let slots_path = dir.join("slots");
    let slots = fs::read(&slots_path).unwrap();
    let mut longer = slots.clone();
    longer.push(0xab);
    fs::write(&slots_path, &longer).unwrap();
    Mempool::open_or_create(&dir).expect("slots longer than the table still open");
    fs::write(&slots_path, &slots[..slots.len() - 1]).unwrap();
    let err = match Mempool::open_or_create(&dir) {
        Err(e) => e,
        Ok(_) => panic!("a short slots file must not open"),
    };
    assert!(
        matches!(err, MempoolError::Corrupt("slots file short")),
        "{err}"
    );
    fs::write(&slots_path, &slots).unwrap();
    let body_path = dir.join("tx.body");
    let body = fs::read(&body_path).unwrap();
    assert_eq!(body.len(), BODY_HEADER);
    fs::write(&body_path, &body[..BODY_HEADER - 1]).unwrap();
    let err = match Mempool::open_or_create(&dir) {
        Err(e) => e,
        Ok(_) => panic!("a short body file must not open"),
    };
    assert!(
        matches!(err, MempoolError::Corrupt("body too short")),
        "{err}"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn persist_due_waits_five_seconds() {
    let dir = tmp_dir();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let tid = Txid::from_byte_array([0x22; 32]);
        put_live(&mut mp, &tid, 1, 400);
        mp.persist_due().unwrap();
        assert_eq!(mp.live_count(), 1);
    }
    {
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0, "admit at t=0 is not durable");
    }
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let tid = Txid::from_byte_array([0x33; 32]);
        put_live(&mut mp, &tid, 1, 400);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        mp.persist_due().unwrap();
    }
    {
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 1, "persist_due after 5s is durable");
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn death_batch_survives_reopen_without_rewriting_body() {
    let dir = tmp_dir();
    let n = 8u32;
    let mut slots = Vec::with_capacity(n as usize);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    for i in 0..n {
        let mut id = [0u8; 32];
        id[0] = i as u8;
        id[1] = 0xD0;
        slots.push(put_live(&mut mp, &Txid::from_byte_array(id), 1, 400));
    }
    mp.flush().unwrap();
    let body_before = fs::read(dir.join("tx.body")).unwrap();
    for slot in &slots {
        mp.mark_slot_dead(*slot).unwrap();
    }
    mp.sync_deaths().unwrap();
    assert_eq!(
        fs::read(dir.join("tx.body")).unwrap(),
        body_before,
        "a death batch must not rewrite tx.body"
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert!(
        mp.load_live_txs().unwrap().is_empty(),
        "batched DEAD marks must not resurrect"
    );
    assert_eq!(mp.live_count(), 0);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn dead_pwrite_of_flushed_slot_is_immediate() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let tid = Txid::from_byte_array([0x11; 32]);
    let slot = put_live(&mut mp, &tid, 1, 400);
    mp.flush().unwrap();
    let body_before = fs::read(dir.join("tx.body")).unwrap();
    mp.mark_slot_dead(slot).unwrap();
    mp.sync_deaths().unwrap();
    assert_eq!(mp.live_count(), 0);
    let body_after = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(
        body_before, body_after,
        "DEAD pwrite must not rewrite tx.body"
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(
        mp.live_count(),
        0,
        "DEAD of a flushed slot is durable without 5s"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reused_dead_slot_sync_does_not_resurrect_the_old_tx() {
    let dir = tmp_dir();
    let old = Txid::from_byte_array([0xA1; 32]);
    let new = Txid::from_byte_array([0xA2; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let slot = put_live(&mut mp, &old, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(slot).unwrap();
        let reused = put_live(&mut mp, &new, 2, 400);
        assert_eq!(reused, slot, "a dead slot is reused");
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    assert!(
        live.iter().all(|tx| tx.packed.txid != old),
        "the evicted tx must not reload"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[cfg(debug_assertions)]
#[test]
fn slot_growth_commit_does_not_stamp_the_replacement_dead() {
    let dir = tmp_dir();
    let old = Txid::from_byte_array([0xE1; 32]);
    let new = Txid::from_byte_array([0xE2; 32]);
    let extra = Txid::from_byte_array([0xE3; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.testing_set_slot_cap(1);
        let slot = put_live(&mut mp, &old, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(slot).unwrap();
        let reused = put_live(&mut mp, &new, 2, 400);
        assert_eq!(reused, slot, "the dead slot is reused");
        let grown = put_live(&mut mp, &extra, 3, 400);
        assert_ne!(grown, slot, "a full live table grows a new slot");
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(
        ids.contains(&new),
        "the growth commit must not leave a death on the reused slot"
    );
    assert!(!ids.contains(&old));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn truncated_flush_mark_is_quarantined_with_the_sidecar() {
    let dir = tmp_dir();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        put_live(&mut mp, &Txid::from_byte_array([0xF1; 32]), 1, 400);
        mp.flush().unwrap();
    }
    fs::write(dir.join("fee_history"), b"keep").unwrap();
    fs::create_dir_all(dir.join("guard")).unwrap();
    fs::write(dir.join("guard/flush"), b"torn").unwrap();
    rbitcoin_mempool::ActiveMempool::open_or_create(&dir)
        .expect("a torn flush mark is quarantined with the sidecar");
    assert!(!dir.join("guard/flush").exists());
    assert!(dir.join("fee_history").is_file());
    let torn = fs::read_dir(&dir)
        .unwrap()
        .filter_map(|ent| ent.ok())
        .find(|ent| ent.file_name().to_string_lossy().starts_with("torn-"))
        .expect("the unreadable mark moves aside with the sidecar")
        .path();
    assert_eq!(
        fs::read(torn.join("guard/flush")).unwrap(),
        b"torn",
        "the quarantine keeps the unreadable mark as it was"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A full flush that commits B in A's reused slot retires A's queued death,
/// even when the `tx.body` trim after the commit fails. A later death sync
/// must not stamp DEAD over B.
#[cfg(debug_assertions)]
#[test]
fn a_committed_full_flush_retires_queued_deaths() {
    use rbitcoin_mempool::store::fail_next_mempool_body_adopt;
    let dir = tmp_dir();
    let old = Txid::from_byte_array([0xC1; 32]);
    let new = Txid::from_byte_array([0xC2; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let slot = put_live(&mut mp, &old, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(slot).unwrap();
        let reused = put_live(&mut mp, &new, 2, 400);
        assert_eq!(reused, slot);
        fail_next_mempool_body_adopt(&dir);
        assert!(mp.flush().is_err());
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(ids.contains(&new), "a queued death stamped the replacement");
    assert!(!ids.contains(&old));
    let _ = fs::remove_dir_all(&dir);
}

/// The timed persist that commits B in A's reused slot retires A's queued
/// death, even when the `tx.body` trim after the commit fails.
#[cfg(debug_assertions)]
#[test]
fn a_committed_timed_persist_retires_queued_deaths() {
    use rbitcoin_mempool::store::fail_next_mempool_body_adopt;
    let dir = tmp_dir();
    let old = Txid::from_byte_array([0xC5; 32]);
    let new = Txid::from_byte_array([0xC6; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let slot = put_live(&mut mp, &old, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(slot).unwrap();
        let reused = put_live(&mut mp, &new, 2, 400);
        assert_eq!(reused, slot);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        fail_next_mempool_body_adopt(&dir);
        assert!(mp.persist_due().is_err());
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(ids.contains(&new), "a queued death stamped the replacement");
    assert!(!ids.contains(&old));
    let _ = fs::remove_dir_all(&dir);
}

/// A slot table that grew on disk but whose sync failed is still not
/// durable. A retry syncs it again before any mark publishes the larger
/// `slot_cap`.
#[cfg(debug_assertions)]
#[test]
fn a_retry_syncs_the_grown_slot_file_again() {
    use rbitcoin_mempool::store::set_mempool_grow_sync_fails;
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    set_mempool_grow_sync_fails(&dir, true);
    assert!(mp.grow_slots().is_err());
    assert!(
        mp.flush().is_err(),
        "a retry skipped the sync of the longer slots file"
    );
    set_mempool_grow_sync_fails(&dir, false);
    mp.flush().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

/// The body only appends. A persist syncs the new records, then writes and
/// syncs the body length, before it arms `guard/flush` for slots and meta.
/// The mark names no `tx.body` page.
#[cfg(debug_assertions)]
#[test]
fn a_persist_appends_the_body_before_the_mark() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let id = Txid::from_byte_array([0xB7; 32]);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    put_live(&mut mp, &id, 1, 400);
    let logical = u64::from_le_bytes(mp.body[8..16].try_into().unwrap());
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    fail_next_mempool_guard_before_write(&dir);
    assert!(mp.persist_due().is_err());
    let mark = fs::read(dir.join("guard/flush")).unwrap();
    assert!(
        !mark.windows(7).any(|w| w == b"tx.body"),
        "the flush mark must not image tx.body"
    );
    let body = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(
        u64::from_le_bytes(body[8..16].try_into().unwrap()),
        logical,
        "the body length must land before the mark"
    );
    drop(mp);
    let _ = fs::remove_dir_all(&dir);
}

/// A persist whose arm fails before the mark is visible has already made
/// the body durable. The retry must still publish the admit's LIVE slot:
/// a crash after it reopens with the admit.
#[cfg(debug_assertions)]
#[test]
fn a_retry_after_an_invisible_arm_failure_publishes_the_slot() {
    let dir = tmp_dir();
    let first = Txid::from_byte_array([0xE0; 32]);
    let second = Txid::from_byte_array([0xE1; 32]);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    put_live(&mut mp, &first, 1, 400);
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    mp.persist_due().unwrap();
    put_live(&mut mp, &second, 2, 400);
    // A file where `guard/` should be: the arm fails before any rename.
    let guard = dir.join("guard");
    let _ = fs::remove_dir_all(&guard);
    fs::write(&guard, b"x").unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
    assert!(mp.persist_due().is_err());
    assert!(!dir.join("guard/flush").exists());
    fs::remove_file(&guard).unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 3);
    mp.persist_due().unwrap();
    // The process dies here: nothing flushes on the way out.
    std::mem::forget(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(live.contains(&first));
    assert!(live.contains(&second), "the retry dropped the admit's slot");
    let _ = fs::remove_dir_all(&dir);
}

/// An admit's body is synced but its arm fails before the mark is visible.
/// A death-only flush then arms and fails. Settling that mark must not move
/// the slot watermark past the admit: its slot is in no mark, so the next
/// persist has to write it, and a crash after that reopens with the admit.
#[cfg(debug_assertions)]
#[test]
fn settling_a_death_mark_keeps_an_unpublished_admit_slot() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let first = Txid::from_byte_array([0xE4; 32]);
    let second = Txid::from_byte_array([0xE5; 32]);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let first_slot = put_live(&mut mp, &first, 1, 400);
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    mp.persist_due().unwrap();
    put_live(&mut mp, &second, 2, 400);
    // A file where `guard/` should be: the arm fails before any rename.
    let guard = dir.join("guard");
    let _ = fs::remove_dir_all(&guard);
    fs::write(&guard, b"x").unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
    assert!(mp.persist_due().is_err());
    fs::remove_file(&guard).unwrap();
    // A death-only flush arms its mark, then fails before its writes.
    mp.mark_slot_dead(first_slot).unwrap();
    fail_next_mempool_guard_before_write(&dir);
    assert!(mp.sync_deaths().is_err());
    assert!(dir.join("guard/flush").is_file());
    mp.set_now_ms(PERSIST_INTERVAL_MS * 3);
    mp.persist_due().unwrap();
    // The process dies here: nothing flushes on the way out.
    std::mem::forget(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(!live.contains(&first));
    assert!(
        live.contains(&second),
        "settling the death mark dropped the admit's slot"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A failed persist leaves `guard/flush` armed with the admit's LIVE slot.
/// Removing that tx before the next persist must reach disk, or open
/// replays the mark and loads the removed tx as LIVE.
#[cfg(debug_assertions)]
#[test]
fn a_removal_supersedes_a_pending_admit_mark() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let gone = Txid::from_byte_array([0xD1; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let slot = put_live(&mut mp, &gone, 1, 400);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        fail_next_mempool_guard_before_write(&dir);
        assert!(mp.persist_due().is_err());
        assert!(dir.join("guard/flush").is_file());
        mp.mark_slot_dead(slot).unwrap();
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(
        !ids.contains(&gone),
        "open replayed the admit after its removal"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The arm can rename `guard/flush` into place and then fail its directory
/// sync. That mark is visible, so a removal of its admit must still publish.
#[cfg(debug_assertions)]
#[test]
fn a_removal_supersedes_a_mark_whose_arm_failed_after_the_rename() {
    use rbitcoin_mempool::store::fail_next_mempool_arm_after_rename;
    let dir = tmp_dir();
    let gone = Txid::from_byte_array([0xD5; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let slot = put_live(&mut mp, &gone, 1, 400);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        fail_next_mempool_arm_after_rename(&dir);
        assert!(mp.persist_due().is_err());
        assert!(dir.join("guard/flush").is_file());
        mp.mark_slot_dead(slot).unwrap();
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(
        !ids.contains(&gone),
        "open replayed the admit after its removal"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Slot 170 starts 16 bytes before the 8 KiB page edge. A failed persist
/// wrote its status and body range on page 4096 but not its txid on page
/// 8192. A later death-only flush of slot 0 must not drop the armed image
/// of page 8192, or open finds a LIVE row with the wrong txid.
#[cfg(debug_assertions)]
#[test]
fn a_death_flush_keeps_the_pending_admit_images() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let first = Txid::from_byte_array([0xE0; 32]);
    let late = Txid::from_byte_array([0xEF; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        assert_eq!(put_live(&mut mp, &first, 1, 400), 0);
        for i in 1..170u32 {
            let mut id = [0xE1; 32];
            id[..4].copy_from_slice(&i.to_le_bytes());
            put_live(&mut mp, &Txid::from_byte_array(id), 1, 400);
        }
        mp.flush().unwrap();
        let slot = put_live(&mut mp, &late, 1, 400);
        assert_eq!(slot, 170);
        let at = SLOTS_HEADER + 170 * SLOT_REC;
        assert_eq!(at, 8176, "the record crosses the page at 8192");
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        fail_next_mempool_guard_before_write(&dir);
        assert!(mp.persist_due().is_err());
        // The first page of the record reached the live file, the second did not.
        let mut slots = OpenOptions::new()
            .write(true)
            .open(dir.join("slots"))
            .unwrap();
        slots.seek(SeekFrom::Start(at as u64)).unwrap();
        slots.write_all(&mp.slots[at..8192]).unwrap();
        drop(slots);
        mp.mark_slot_dead(0).unwrap();
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).expect("open after the death flush");
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(!ids.contains(&first), "slot 0 is dead");
    assert!(ids.contains(&late), "the armed admit is applied whole");
    let _ = fs::remove_dir_all(&dir);
}

/// Once a later commit has applied an earlier failed persist, its admit is
/// on disk. A removal of that admit must still reach disk.
#[cfg(debug_assertions)]
#[test]
fn a_removal_after_a_settled_mark_stays_dead() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let durable = Txid::from_byte_array([0xF5; 32]);
    let gone = Txid::from_byte_array([0xF6; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let first = put_live(&mut mp, &durable, 1, 400);
        mp.flush().unwrap();
        let slot = put_live(&mut mp, &gone, 1, 400);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        fail_next_mempool_guard_before_write(&dir);
        assert!(mp.persist_due().is_err());
        mp.mark_slot_dead(first).unwrap();
        mp.sync_deaths().unwrap();
        mp.mark_slot_dead(slot).unwrap();
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let ids: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert!(!ids.contains(&durable));
    assert!(
        !ids.contains(&gone),
        "open loaded the admit after its removal"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn reused_dead_slot_persist_due_publishes_the_new_tx() {
    let dir = tmp_dir();
    let old = Txid::from_byte_array([0xB1; 32]);
    let new = Txid::from_byte_array([0xB2; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let slot = put_live(&mut mp, &old, 1, 400);
        mp.flush().unwrap();
        mp.set_now_ms(0);
        mp.mark_slot_dead(slot).unwrap();
        let reused = put_live(&mut mp, &new, 2, 400);
        assert_eq!(reused, slot);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        mp.persist_due().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    let ids: Vec<_> = live.iter().map(|tx| tx.packed.txid).collect();
    assert!(ids.contains(&new), "the replacement must be durable");
    assert!(!ids.contains(&old), "the evicted tx must stay gone");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn compact_then_admit_does_not_publish_a_stale_death() {
    let dir = tmp_dir();
    let keep = Txid::from_byte_array([0xC1; 32]);
    let gone = Txid::from_byte_array([0xC2; 32]);
    let later = Txid::from_byte_array([0xC3; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        put_live(&mut mp, &keep, 1, 400);
        let dead = put_live(&mut mp, &gone, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(dead).unwrap();
        mp.compact().unwrap();
        let admitted = put_live(&mut mp, &later, 3, 400);
        assert_eq!(admitted, dead, "the freed index is reused");
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp
        .load_live_txs()
        .expect("a stale death must not publish a zero-length live row");
    let ids: Vec<_> = live.iter().map(|tx| tx.packed.txid).collect();
    assert!(ids.contains(&keep), "the packed survivor stays live");
    assert!(!ids.contains(&gone));
    assert!(
        !ids.contains(&later),
        "the new admit waits for a body persist"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn compact_clears_a_death_index_that_now_names_the_survivor() {
    let dir = tmp_dir();
    let keep = Txid::from_byte_array([0xD1; 32]);
    let gone = Txid::from_byte_array([0xD2; 32]);
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        let dead = put_live(&mut mp, &gone, 1, 400);
        put_live(&mut mp, &keep, 1, 400);
        mp.flush().unwrap();
        mp.mark_slot_dead(dead).unwrap();
        mp.compact().unwrap();
        mp.sync_deaths().unwrap();
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    let ids: Vec<_> = live.iter().map(|tx| tx.packed.txid).collect();
    assert!(ids.contains(&keep), "the packed survivor stays live");
    assert!(!ids.contains(&gone));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn dead_of_unpersisted_admit_does_not_resurrect() {
    let dir = tmp_dir();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let tid = Txid::from_byte_array([0x44; 32]);
        let slot = put_live(&mut mp, &tid, 1, 400);
        mp.mark_slot_dead(slot).unwrap();
        mp.persist_slots_without_body().unwrap();
    }
    {
        let mp = Mempool::open_or_create(&dir).unwrap();
        assert_eq!(mp.live_count(), 0);
        assert!(
            mp.load_live_txs().unwrap().is_empty(),
            "unpersisted admit DEAD must not resurrect"
        );
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn persist_slots_without_tail_does_not_point_live_past_body() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    let persisted = mp.body_persisted_len();
    mp.persist_slots_without_body().unwrap();
    let on_disk = fs::read(dir.join("slots")).unwrap();
    assert_eq!(
        on_disk[SLOTS_HEADER], SLOT_FREE,
        "a live row past the durable body is free on disk"
    );
    assert_eq!(
        &on_disk[SLOTS_HEADER + 16..SLOTS_HEADER + 48],
        t1.as_byte_array(),
        "demotion keeps the txid and only clears LIVE"
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp
        .load_live_txs()
        .expect("FREE-not-LIVE past EOF must load");
    assert!(
        live.is_empty(),
        "LIVE must not be written past body_persisted_len={persisted}"
    );
    assert_eq!(
        mp.slots[SLOTS_HEADER], SLOT_FREE,
        "a torn LIVE slot past the body is free after open"
    );
    let _ = fs::remove_dir_all(&dir);

    // Slots can reach disk ahead of the published body length. Demoted
    // writes do not create that image; a crash between the two files does.
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let raw = tiny_tx();
    let torn = Txid::from_byte_array([0x11; 32]);
    mp.append_live_tx(
        &raw,
        &torn,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.flush().unwrap();
    drop(mp);
    let mut body = fs::read(dir.join("tx.body")).unwrap();
    let published = u64::from_le_bytes(body[8..16].try_into().unwrap());
    assert!(published > BODY_HEADER as u64);
    body[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
    fs::write(dir.join("tx.body"), &body).unwrap();
    assert_eq!(
        fs::read(dir.join("slots")).unwrap()[SLOTS_HEADER],
        SLOT_LIVE,
        "the slots file still names the row live"
    );
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert!(
        mp.load_live_txs().unwrap().is_empty(),
        "a row past the published body is not a live tx"
    );
    assert_eq!(
        mp.slots[SLOTS_HEADER], SLOT_FREE,
        "open frees a live row whose body was not published"
    );
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn persist_deaths_writes_slots_not_body() {
    let dir = tmp_dir();
    let n = 8u32;
    let mut slots = Vec::with_capacity(n as usize);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    for i in 0..n {
        let mut id = [0u8; 32];
        id[0] = i as u8;
        id[1] = (i >> 8) as u8;
        let tid = Txid::from_byte_array(id);
        slots.push(put_live(&mut mp, &tid, 1, 400));
    }
    mp.flush().unwrap();
    let body_before = fs::read(dir.join("tx.body")).unwrap();
    for slot in &slots {
        mp.mark_slot_dead(*slot).unwrap();
    }
    mp.persist_if_dirty().unwrap();
    let body_after = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(
        body_before, body_after,
        "DEAD persist must not rewrite mempool/tx.body"
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.live_count(), 0);
    let _ = fs::remove_dir_all(&dir);
}

#[cfg(debug_assertions)]
#[test]
fn torn_mempool_page_restores_the_armed_image() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    mp.persist_due().unwrap();
    let t2 = Txid::from_byte_array([0x02; 32]);
    mp.append_live_tx(
        &raw,
        &t2,
        &raw.compute_wtxid(),
        2,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
    fail_next_mempool_guard_before_write(&dir);
    assert!(mp.persist_due().is_err());
    assert!(dir.join("guard/flush").is_file());
    drop(mp);
    // The body only appends and is not imaged. The slots page is.
    let mut slots = OpenOptions::new()
        .write(true)
        .open(dir.join("slots"))
        .unwrap();
    slots.seek(SeekFrom::Start(0)).unwrap();
    slots.write_all(&[0xA5; 4096]).unwrap();
    slots.sync_all().unwrap();
    drop(slots);
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live.len(), 2, "open copies the armed slots page");
    assert!(live.iter().any(|row| row.packed.txid == t2));
    assert!(!dir.join("guard/flush").exists());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn incremental_body_persist_writes_only_dirty_tail() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    mp.persist_due().unwrap();
    let first_end = mp.body_persisted_len();
    let body_after_first = fs::read(dir.join("tx.body")).unwrap();
    let t2 = Txid::from_byte_array([0x02; 32]);
    mp.append_live_tx(
        &raw,
        &t2,
        &raw.compute_wtxid(),
        2,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
    mp.persist_due().unwrap();
    let body_after_second = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(
        &body_after_second[BODY_HEADER..first_end as usize],
        &body_after_first[BODY_HEADER..],
        "first payload bytes must be unchanged"
    );
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.load_live_txs().unwrap().len(), 2);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn persist_due_pwrites_only_new_live_slots() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.set_now_ms(0);
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.set_now_ms(PERSIST_INTERVAL_MS);
    mp.persist_due().unwrap();
    let t2 = Txid::from_byte_array([0x02; 32]);
    mp.append_live_tx(
        &raw,
        &t2,
        &raw.compute_wtxid(),
        2,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    let t3 = Txid::from_byte_array([0x03; 32]);
    mp.append_live_tx(
        &raw,
        &t3,
        &raw.compute_wtxid(),
        3,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    // A byte just past the new slot records. A widened record range rewrites it.
    let past = SLOTS_HEADER + 3 * SLOT_REC;
    {
        let mut slots = OpenOptions::new()
            .write(true)
            .open(dir.join("slots"))
            .unwrap();
        slots.seek(SeekFrom::Start(past as u64)).unwrap();
        slots.write_all(&[0xab]).unwrap();
        slots.sync_all().unwrap();
    }
    // Cap the walk at the live rows so a divided slot offset cannot alias
    // slot 1 from an index thousands of rows later.
    mp.testing_pin_full_slot_table();
    mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
    mp.persist_due().unwrap();
    let on_disk = fs::read(dir.join("slots")).unwrap();
    assert_eq!(
        on_disk[past], 0xab,
        "persist must not rewrite the slot past the new records"
    );
    assert_eq!(on_disk[SLOTS_HEADER + SLOT_REC], SLOT_LIVE);
    assert_eq!(on_disk[SLOTS_HEADER + 2 * SLOT_REC], SLOT_LIVE);
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.load_live_txs().unwrap().len(), 3);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn flush_bumps_generation_monotone() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    for i in 1..=5 {
        mp.flush().unwrap();
        assert_eq!(mp.generation(), i);
    }
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.generation(), 5);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn abandon_live_clears_reopen() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let tid = Txid::from_byte_array([0x22; 32]);
    put_live(&mut mp, &tid, 1, 400);
    mp.flush().unwrap();
    assert_eq!(mp.live_count(), 1);
    assert_eq!(mp.abandon_live().unwrap(), 1);
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.live_count(), 0);
    assert!(mp.load_live_txs().unwrap().is_empty());
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn append_mark_dead_stats_and_bad_magic() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let txid = Txid::from_byte_array([0x11; 32]);
    let slot = mp
        .append_live_tx(
            &tiny_tx(),
            &txid,
            &tiny_tx().compute_wtxid(),
            10,
            400,
            0,
            1_700_000_000,
            &[],
        )
        .unwrap();
    assert_eq!(mp.live_count(), 1);
    let (free, live, dead) = mp.slot_stats();
    assert_eq!(live, 1);
    assert!(free + live + dead >= 1);
    mp.flush().unwrap();
    drop(mp);
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.live_count(), 1);
    let (free2, live2, _dead2) = mp.slot_stats();
    assert_eq!(live2, 1);
    assert!(free2 + live2 >= 1);
    mp.mark_slot_dead(slot).unwrap();
    assert_eq!(mp.live_count(), 0);
    assert!(mp.mark_slot_dead(u32::MAX).is_err());
    drop(mp);

    let dir2 = tmp_dir();
    {
        let _ = Mempool::open_or_create(&dir2).unwrap();
    }
    {
        let mut f = fs::OpenOptions::new()
            .write(true)
            .open(dir2.join("meta"))
            .unwrap();
        f.write_all(b"BAD!").unwrap();
    }
    match Mempool::open_or_create(&dir2) {
        Err(MempoolError::BadMagic) => {}
        Ok(_) => panic!("expected BadMagic, got Ok"),
        Err(e) => panic!("expected BadMagic, got {e}"),
    }
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_dir_all(&dir2);
}

/// Legacy tiny slot table must grow instead of returning Corrupt("slot table full").
#[test]
fn full_live_table_grows_not_corrupt() {
    let dir = tmp_dir();
    // Seed a 4-slot sidecar (legacy mainnet shape).
    fs::create_dir_all(&dir).unwrap();
    let tiny = 4u32;
    {
        let mut meta = [0u8; META_LEN];
        write_meta_bytes(&mut meta, 0, tiny, 0);
        fs::write(dir.join("meta"), meta).unwrap();
        let mut slots = vec![0u8; SLOTS_HEADER + (tiny as usize) * SLOT_REC];
        slots[0..4].copy_from_slice(&MEM_MAGIC);
        slots[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        slots[8..12].copy_from_slice(&tiny.to_le_bytes());
        fs::write(dir.join("slots"), &slots).unwrap();
        let mut body = vec![0u8; BODY_HEADER];
        body[0..4].copy_from_slice(&MEM_MAGIC);
        body[4..6].copy_from_slice(&MEM_SCHEMA.to_le_bytes());
        body[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
        fs::write(dir.join("tx.body"), &body).unwrap();
    }
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.meta().slot_cap, tiny);
    for i in 0..tiny {
        let mut tid = [0u8; 32];
        tid[0] = i as u8 + 1;
        let txid = Txid::from_byte_array(tid);
        put_live(&mut mp, &txid, 1, 400);
    }
    assert!(!mp.has_free_slot());
    // 5th append must grow, not Corrupt.
    let tid5 = Txid::from_byte_array([0x55; 32]);
    let tx = tiny_tx();
    let r = mp.append_live_tx(
        &tx,
        &tid5,
        &tx.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    );
    assert!(
        r.is_ok(),
        "expected grow on full table, got {:?}",
        r.err().map(|e| e.to_string())
    );
    assert_eq!(mp.meta().slot_cap, 16_388);
    assert_eq!(mp.live_count(), tiny + 1);
    drop(mp);
    assert_eq!(
        fs::metadata(dir.join("slots")).unwrap().len(),
        786_640,
        "grown slots file is the 16-byte header plus 16388 records"
    );
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.meta().slot_cap, 16_388);
    // Grow persists the four rows already in the table. The fifth slot is
    // RAM until the next sidecar write, so a reopen keeps those four.
    assert_eq!(mp.load_live_txs().unwrap().len(), 4);
    assert!(!format!("{:?}", MempoolError::Full).contains("corrupt"));
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn persist_new_body_old_slots_reopens() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.flush().unwrap();
    let slots_first = fs::read(dir.join("slots")).unwrap();
    let t2 = Txid::from_byte_array([0x02; 32]);
    mp.append_live_tx(
        &raw,
        &t2,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.flush().unwrap();
    drop(mp);
    fs::write(dir.join("slots"), &slots_first).unwrap();
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp.load_live_txs().expect("new body + old slots must load");
    assert!(live.len() <= 1);
    let _ = fs::remove_dir_all(&dir);

    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    let off = SLOTS_HEADER;
    mp.slots[off + 12..off + 16].copy_from_slice(&88u32.to_le_bytes());
    let short = mp.load_live_txs().unwrap_err().to_string();
    assert!(
        !short.contains("live slot body range"),
        "an 88-byte live record is past the prefix check: {short}"
    );
    mp.slots[off + 12..off + 16].copy_from_slice(&87u32.to_le_bytes());
    let shorter = mp.load_live_txs().unwrap_err().to_string();
    assert!(shorter.contains("live slot body range"), "{shorter}");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn compact_both_tmps_discarded_on_open() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    mp.flush().unwrap();
    drop(mp);
    fs::copy(dir.join("tx.body"), dir.join("tx.body.tmp")).unwrap();
    fs::copy(dir.join("slots"), dir.join("slots.tmp")).unwrap();
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert!(!dir.join("tx.body.tmp").exists());
    assert!(!dir.join("slots.tmp").exists());
    mp.load_live_txs().unwrap();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn compact_crash_after_slots_rename_before_meta_still_loads() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let raw = tiny_tx();
    let t1 = Txid::from_byte_array([0x01; 32]);
    mp.append_live_tx(
        &raw,
        &t1,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    let t2 = Txid::from_byte_array([0x02; 32]);
    mp.append_live_tx(
        &raw,
        &t2,
        &raw.compute_wtxid(),
        1,
        400,
        0,
        1_700_000_000,
        &[],
    )
    .unwrap();
    mp.flush().unwrap();
    assert_eq!(mp.live_count(), 2);
    let meta_before_compact = fs::read(dir.join("meta")).unwrap();
    mp.mark_slot_dead(0).unwrap();
    mp.compact().unwrap();
    drop(mp);
    fs::write(dir.join("meta"), &meta_before_compact).unwrap();
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live = mp
        .load_live_txs()
        .expect("packed images + stale live_count must load");
    assert_eq!(live.len(), 1, "packed live set");
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn leftover_schema_v1_converts_on_open() {
    use bitcoin::consensus::encode::serialize;
    let dir = tmp_dir();
    fs::create_dir_all(&dir).unwrap();
    let tx = tiny_tx();
    let raw = serialize(&tx);
    let tid = tx.compute_txid();
    let fee = 123u64;
    let weight = 400u64;
    {
        let mut meta = [0u8; META_LEN];
        write_meta_bytes(&mut meta, 0, 4, 1);
        meta[4..6].copy_from_slice(&1u16.to_le_bytes());
        fs::write(dir.join("meta"), meta).unwrap();
        let mut slots = vec![0u8; SLOTS_HEADER + 4 * SLOT_REC];
        slots[0..4].copy_from_slice(&MEM_MAGIC);
        slots[4..6].copy_from_slice(&1u16.to_le_bytes());
        slots[8..12].copy_from_slice(&4u32.to_le_bytes());
        let off = SLOTS_HEADER;
        slots[off] = SLOT_LIVE;
        let body_off = BODY_HEADER as u64;
        let body_len = (16 + raw.len()) as u32;
        slots[off + 4..off + 12].copy_from_slice(&body_off.to_le_bytes());
        slots[off + 12..off + 16].copy_from_slice(&body_len.to_le_bytes());
        slots[off + 16..off + 48].copy_from_slice(tid.as_byte_array());
        fs::write(dir.join("slots"), &slots).unwrap();
        let mut body = vec![0u8; BODY_HEADER + body_len as usize];
        body[0..4].copy_from_slice(&MEM_MAGIC);
        body[4..6].copy_from_slice(&1u16.to_le_bytes());
        body[8..16].copy_from_slice(&((BODY_HEADER + body_len as usize) as u64).to_le_bytes());
        body[BODY_HEADER..BODY_HEADER + 8].copy_from_slice(&fee.to_le_bytes());
        body[BODY_HEADER + 8..BODY_HEADER + 16].copy_from_slice(&weight.to_le_bytes());
        body[BODY_HEADER + 16..].copy_from_slice(&raw);
        fs::write(dir.join("tx.body"), &body).unwrap();
    }
    {
        let mp = Mempool::open_or_create(&dir).expect("v1 leftover converts");
        let live = mp.load_live_txs().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].packed.fee_sat, fee);
        assert_eq!(live[0].packed.weight, weight);
        assert_eq!(live[0].packed.txid, tid);
        assert_eq!(live[0].packed.wtxid, tx.compute_wtxid());
        assert_eq!(serialize(&live[0].packed.tx), raw);
        assert!(live[0]
            .packed
            .vins
            .iter()
            .all(|v| v.script_hash.is_none() && v.create_fk.is_none()));
    }
    let meta = fs::read(dir.join("meta")).unwrap();
    assert_eq!(
        &meta[4..6],
        &MEM_SCHEMA.to_le_bytes(),
        "meta stamped current schema"
    );
    let body = fs::read(dir.join("tx.body")).unwrap();
    assert_eq!(
        &body[4..6],
        &MEM_SCHEMA.to_le_bytes(),
        "body stamped current schema"
    );
    {
        let mp = Mempool::open_or_create(&dir).unwrap();
        let live = mp.load_live_txs().unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].packed.txid, tid);
        assert_eq!(serialize(&live[0].packed.tx), raw);
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn leftover_schema_unknown_is_refused() {
    let dir = tmp_dir();
    let next = MEM_SCHEMA + 1;
    fs::create_dir_all(&dir).unwrap();
    {
        let mut meta = [0u8; META_LEN];
        write_meta_bytes(&mut meta, 0, 4, 0);
        meta[4..6].copy_from_slice(&next.to_le_bytes());
        fs::write(dir.join("meta"), meta).unwrap();
        let mut slots = vec![0u8; SLOTS_HEADER + 4 * SLOT_REC];
        slots[0..4].copy_from_slice(&MEM_MAGIC);
        slots[4..6].copy_from_slice(&next.to_le_bytes());
        fs::write(dir.join("slots"), &slots).unwrap();
        let mut body = vec![0u8; BODY_HEADER];
        body[0..4].copy_from_slice(&MEM_MAGIC);
        body[4..6].copy_from_slice(&next.to_le_bytes());
        body[8..16].copy_from_slice(&(BODY_HEADER as u64).to_le_bytes());
        fs::write(dir.join("tx.body"), &body).unwrap();
    }
    match Mempool::open_or_create(&dir) {
        Err(MempoolError::BadSchema(s)) if s == next => {}
        Ok(_) => panic!("expected BadSchema({next}), got Ok"),
        Err(e) => panic!("expected BadSchema({next}), got {e}"),
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn open_or_init_meta_reads_written_fields_and_rejects_bad_files() {
    let dir = tmp_dir();
    let path = dir.join("meta");
    let mut buf = [0u8; META_LEN];
    write_meta_bytes(&mut buf, 17, 8, 3);
    fs::write(&path, buf).unwrap();
    let (_file, gen, cap, live, schema) = match open_or_init_meta(&path) {
        Ok(opened) => opened,
        Err(err) => panic!("{err}"),
    };
    assert_eq!((gen, cap, live, schema), (17, 8, 3, MEM_SCHEMA));
    write_meta_bytes(&mut buf, 1, 0, 0);
    fs::write(&path, buf).unwrap();
    match open_or_init_meta(&path) {
        Err(MempoolError::Corrupt(m)) => assert!(m.contains("slot_cap zero"), "{m}"),
        Err(err) => panic!("{err}"),
        Ok(_) => panic!("zero slot_cap opened"),
    }
    let mut junk = [0u8; META_LEN];
    junk[0..4].copy_from_slice(b"BAD!");
    fs::write(&path, junk).unwrap();
    match open_or_init_meta(&path) {
        Err(MempoolError::BadMagic) => {}
        Err(err) => panic!("{err}"),
        Ok(_) => panic!("bad magic opened"),
    }
    fs::write(&path, b"rBMP").unwrap();
    match open_or_init_meta(&path) {
        Err(MempoolError::Io { .. }) => {}
        Err(err) => panic!("{err}"),
        Ok(_) => panic!("short meta opened"),
    }
    let fresh = dir.join("missing-meta");
    let (_file, gen, cap, live, schema) = match open_or_init_meta(&fresh) {
        Ok(opened) => opened,
        Err(err) => panic!("{err}"),
    };
    assert_eq!(
        (gen, cap, live, schema),
        (0, DEFAULT_SLOT_CAP, 0, MEM_SCHEMA)
    );
}

/// Two new slot rows share one `pwrite` when they are adjacent. Separate
/// writes leave the same bytes a reopen reads. The merge is the persist
/// path for a burst of admits.
#[test]
fn adjacent_slot_records_touch_when_the_next_starts_at_the_end() {
    assert!(Mempool::adjacent_slot_records(64, 64));
    assert!(!Mempool::adjacent_slot_records(64, 112));
}

#[test]
fn abandon_live_clears_every_slot_on_reopen() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    put_live(&mut mp, &Txid::from_byte_array([0x81; 32]), 1, 400);
    put_live(&mut mp, &Txid::from_byte_array([0x82; 32]), 1, 400);
    mp.flush().unwrap();
    // Two live rows and a cap of 2: slot 1 is only reached by `slot * SLOT_REC`.
    mp.testing_pin_full_slot_table();
    assert_eq!(mp.abandon_live().unwrap(), 2);
    drop(mp);
    let mp = Mempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.live_count(), 0);
    assert!(mp.load_live_txs().unwrap().is_empty());
    let _ = fs::remove_dir_all(&dir);
}

/// Free, live, and dead slot counts. No RPC returns the split. Eviction
/// and compact both read it to decide whether a slot is reusable.
#[test]
fn slot_stats_counts_free_live_and_dead() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let cap = mp.meta().slot_cap;
    let slot = put_live(&mut mp, &Txid::from_byte_array([0x91; 32]), 1, 400);
    let (free, live, dead) = mp.slot_stats();
    assert_eq!((free, live, dead), (cap - 1, 1, 0));
    mp.mark_slot_dead(slot).unwrap();
    let (free, live, dead) = mp.slot_stats();
    assert_eq!((free, live, dead), (cap - 1, 0, 1));
    let _ = fs::remove_dir_all(&dir);
}

/// A live slot whose body length runs past the file. Compact refuses it.
/// A normal reopen never builds this row. Startup compact of a torn
/// slots file is the path that returns the error.
#[test]
fn compact_rejects_a_live_slot_whose_body_range_is_not_in_the_file() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    put_live(&mut mp, &Txid::from_byte_array([0xa1; 32]), 1, 400);
    let off = SLOTS_HEADER;
    // A prefix-sized span inside the file is a copy, not a range error.
    mp.slots[off + 12..off + 16].copy_from_slice(&(BODY_TX_PREFIX as u32).to_le_bytes());
    let (n, _) = mp.compact().unwrap();
    assert_eq!(n, 1);

    put_live(&mut mp, &Txid::from_byte_array([0xa2; 32]), 1, 400);
    let off = SLOTS_HEADER;
    let logical = mp.body_logical_len().unwrap();
    let past = (logical as u32).saturating_add(8);
    mp.slots[off + 12..off + 16].copy_from_slice(&past.to_le_bytes());
    match mp.compact() {
        Err(MempoolError::Corrupt("live slot body range")) => {}
        other => panic!("expected live slot body range, got {other:?}"),
    }
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn body_logical_len_reads_the_word_and_rejects_short() {
    let mut body = vec![0u8; BODY_HEADER];
    body[8..16].copy_from_slice(&24u64.to_le_bytes());
    body.extend_from_slice(&[0u8; 8]);
    assert_eq!(body_logical_len(&body).unwrap(), 24);
    match body_logical_len(&body[..8]) {
        Err(MempoolError::Corrupt("body too short")) => {}
        Err(err) => panic!("{err}"),
        Ok(_) => panic!("a short body reported a length"),
    }
    body[8..16].copy_from_slice(&4u64.to_le_bytes());
    match body_logical_len(&body) {
        Err(MempoolError::Corrupt("body logical len")) => {}
        Err(err) => panic!("{err}"),
        Ok(_) => panic!("a logical length under the header was accepted"),
    }
}

/// Accept time is the named slot's record. Slot 0 and slot 1 share an
/// offset when the stride is divided away.
#[test]
fn live_accept_secs_reads_the_named_slot() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let tx = tiny_tx();
    let a = Txid::from_byte_array([0xa1; 32]);
    let b = Txid::from_byte_array([0xb2; 32]);
    let s0 = mp
        .append_live_tx(&tx, &a, &tx.compute_wtxid(), 1, 80, 0, 11, &[])
        .unwrap();
    let s1 = mp
        .append_live_tx(&tx, &b, &tx.compute_wtxid(), 1, 80, 0, 22, &[])
        .unwrap();
    assert_eq!(mp.live_accept_secs(s0), Some(11));
    assert_eq!(mp.live_accept_secs(s1), Some(22));
    let _ = fs::remove_dir_all(&dir);
}

/// The logical cap is the image's record count. Compaction reads that cap
/// (`testing_set_slot_cap` in the admit path). One past the image is refused.
#[test]
fn slot_cap_matches_the_image_stride() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let fit = ((mp.slots.len() - SLOTS_HEADER) / SLOT_REC) as u32;
    mp.testing_set_slot_cap(fit);
    assert_eq!(mp.meta().slot_cap, fit);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
#[should_panic(expected = "slot image is shorter than cap")]
fn slot_cap_refuses_one_past_the_image() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let fit = ((mp.slots.len() - SLOTS_HEADER) / SLOT_REC) as u32;
    mp.testing_set_slot_cap(fit + 1);
}

/// The trust byte is slot 1's own record. Reopen of a warned tx reads it
/// (`trust_kept` survives restart).
#[test]
fn trust_flag_lands_on_its_own_slot() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let a = Txid::from_byte_array([0xa3; 32]);
    let b = Txid::from_byte_array([0xb4; 32]);
    let s0 = put_live(&mut mp, &a, 1, 80);
    let s1 = put_live(&mut mp, &b, 1, 80);
    mp.set_slot_trust_kept(s1, true);
    let live = mp.load_live_txs().unwrap();
    let row = |slot: u32| live.iter().find(|t| t.slot == slot).unwrap();
    assert!(!row(s0).trust_kept);
    assert!(row(s1).trust_kept);
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn live_slot_accept_secs_and_txid_roundtrip() {
    let dir = tmp_dir();
    let mut mp = Mempool::open_or_create(&dir).unwrap();
    let tx = tiny_tx();
    let tid = tx.compute_txid();
    let slot = put_live(&mut mp, &tid, 15, 80);
    assert_eq!(mp.live_accept_secs(slot), Some(1_700_000_000));
    let live = mp.load_live_txs().unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(live[0].packed.txid, tid);
    assert_eq!(live[0].packed.fee_sat, 15);
    assert_eq!(live[0].slot, slot);
}

/// A persist that failed after its arm already appended and synced the
/// body. The retry applies the armed mark, appends nothing more, and still
/// publishes every admit across two body pages.
#[cfg(debug_assertions)]
#[test]
fn a_retried_persist_after_a_failed_mark_keeps_every_admit() {
    use rbitcoin_mempool::store::fail_next_mempool_guard_before_write;
    let dir = tmp_dir();
    let mut ids = Vec::new();
    {
        let mut mp = Mempool::open_or_create(&dir).unwrap();
        mp.set_now_ms(0);
        let first = Txid::from_byte_array([0xB0; 32]);
        put_live(&mut mp, &first, 1, 400);
        ids.push(first);
        mp.set_now_ms(PERSIST_INTERVAL_MS);
        mp.persist_due().unwrap();
        let mut n = 1u32;
        while mp.body_logical_len().unwrap() < 2 * 4096 {
            let mut id = [0xB1; 32];
            id[..4].copy_from_slice(&n.to_le_bytes());
            let txid = Txid::from_byte_array(id);
            put_live(&mut mp, &txid, 1, 400);
            ids.push(txid);
            n += 1;
        }
        mp.set_now_ms(PERSIST_INTERVAL_MS * 2);
        fail_next_mempool_guard_before_write(&dir);
        assert!(mp.persist_due().is_err());
        mp.persist_due().expect("the retry publishes the admits");
    }
    let mp = Mempool::open_or_create(&dir).unwrap();
    let live: Vec<_> = mp
        .load_live_txs()
        .unwrap()
        .iter()
        .map(|tx| tx.packed.txid)
        .collect();
    assert_eq!(live.len(), ids.len());
    assert!(ids.iter().all(|id| live.contains(id)));
    let _ = fs::remove_dir_all(&dir);
}
