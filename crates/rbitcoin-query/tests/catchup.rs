use rbitcoin_primitives::{Fk, Height};
use rbitcoin_query::catchup::*;
use rbitcoin_query::sh_builder::{load_seal, store_seal};
use rbitcoin_query::testutil::FixtureChain;
use rbitcoin_query::Query;
use rbitcoin_store::StoreError;
use rbitcoin_store::{HeaderRecord, InputRecord, OutputRecord, TxRecord};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn coinbase_block(
    h: u32,
    prev: Fk,
    parent_hash: Option<[u8; 32]>,
) -> (HeaderRecord, rbitcoin_query::TxApply) {
    let version = 1;
    let timestamp = h + 1;
    let bits = 0x207fffff;
    let nonce = h;
    let mut txid = [0u8; 32];
    txid[0..4].copy_from_slice(&h.to_le_bytes());
    txid[31] = 0xcb;
    // One transaction: the merkle root is its txid, so a reopen's tip
    // revalidation keeps the chain.
    let merkle = txid;
    let ph = parent_hash.unwrap_or([0u8; 32]);
    let hash = rbitcoin_store::block_header_hash(version, &ph, &merkle, timestamp, bits, nonce);
    let header = HeaderRecord {
        prev_fk: prev,
        version,
        timestamp,
        bits,
        nonce,
        merkle_root: merkle,
        hash,
        size: 0,
        weight: 0,
    };
    let ta = rbitcoin_query::TxApply {
        tx: TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_count: 1,
            output_count: 1,
        },
        inputs: vec![InputRecord {
            prev_txid: [0u8; 32],
            parent: rbitcoin_store::InputParent::Coinbase,
            sequence: u32::MAX,
            script_sig: vec![h as u8],
            witness: vec![],
        }],
        outputs: vec![OutputRecord::unspent(50_0000_0000, vec![0x51, h as u8])],
    };
    (header, ta)
}

#[test]
fn unknown_index_mode_byte_is_corrupt() {
    let (_dir, q) = rbitcoin_query::testutil::tiny_query_labeled("idx-mode-byte");
    for bad in [0u8, 3] {
        q.index_mode_cell
            .store(bad, std::sync::atomic::Ordering::SeqCst);
        match q.index_mode() {
            Err(StoreError::Corrupt(msg)) => {
                assert_eq!(msg, "index mode byte", "byte {bad}");
            }
            other => panic!("byte {bad} must be corrupt, got {other:?}"),
        }
    }
    q.index_mode_cell
        .store(IndexMode::Direct as u8, std::sync::atomic::Ordering::SeqCst);
    assert!(q.index_mode().expect("direct").is_direct());
    q.index_mode_cell
        .store(IndexMode::Tip as u8, std::sync::atomic::Ordering::SeqCst);
    assert!(q.index_mode().expect("tip").is_tip());
    assert!(!q.index_mode().expect("tip").is_direct());
}

fn seed_direct_chain(q: &Query, n: u32) {
    q.enter_direct_index_mode().unwrap();
    extend_direct_chain(q, n);
    assert_eq!(q.tip_height(), Some(Height(n - 1)));
    assert!(q.store.txs.count() >= u64::from(n));
}

fn extend_direct_chain(q: &Query, add: u32) {
    if add == 0 {
        return;
    }
    let start = q.tip_height().map(|h| h.0.saturating_add(1)).unwrap_or(0);
    let mut prev = Fk::NULL;
    let mut parent_hash: Option<[u8; 32]> = None;
    if start > 0 {
        let prev_h = Height(start - 1);
        prev = q.store.confirmed.get(prev_h).unwrap().unwrap();
        parent_hash = Some(q.get_header(prev).unwrap().hash);
    }
    for h in start..start + add {
        let (header, ta) = coinbase_block(h, prev, parent_hash);
        parent_hash = Some(header.hash);
        prev = q.connect_block(Height(h), &header, &[ta]).unwrap();
    }
}

#[test]
fn finalize_sh_runs_durable_head_missing_hwm_keeps_seal() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-hwm");
    q.enter_direct_index_mode().unwrap();
    seed_direct_chain(&q, 3);
    let n0 = q.finalize_sh_runs().unwrap();
    assert!(n0 >= 1 || q.store.scripthash.has_durable_index());
    assert!(q.store.scripthash.has_durable_index());
    let count_before = q.store.scripthash.entry_count();
    let sh0 = rbitcoin_store::script_hash(&[0x51, 0]);

    let runs_dir = dir.join("scripthash.runs");
    std::fs::create_dir_all(&runs_dir).unwrap();
    // A SEAL past the archive is an index past it, which is recollected.
    let high_seal = q.store.txs.count();
    store_seal(&runs_dir, high_seal).unwrap();
    q.sh_run.refresh_seal();
    let _ = std::fs::remove_file(dir.join(rbitcoin_store::INCLUDE_HWM_NAME));
    assert_eq!(q.store.scripthash.include_hwm(), 0);
    assert_eq!(q.sh_run.sealed_max_create_fk(), high_seal);

    let _ = q.finalize_sh_runs().unwrap();
    assert_eq!(
        q.sh_run.sealed_max_create_fk(),
        high_seal,
        "SEAL must not be clamped to 0 when HWM was missing"
    );
    assert_eq!(
        q.store.scripthash.include_hwm(),
        high_seal,
        "include_hwm must bootstrap from SEAL"
    );
    assert!(
        !q.store.scripthash.entries(&sh0).unwrap().is_empty(),
        "durable head must remain"
    );
    assert!(q.store.scripthash.entry_count() >= count_before);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn finalize_sh_runs_empty_head_leftover_seal_is_wiped() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-stale");
    q.enter_direct_index_mode().unwrap();
    assert!(!q.store.scripthash.has_durable_index());

    let runs_dir = dir.join("scripthash.runs");
    std::fs::create_dir_all(&runs_dir).unwrap();
    let high_seal = 1_400_000_000u64;
    store_seal(&runs_dir, high_seal).unwrap();
    q.sh_run.refresh_seal();
    std::fs::write(runs_dir.join("000001.run"), b"leftover").unwrap();

    let _ = q.finalize_sh_runs().unwrap();
    assert_eq!(q.sh_run.sealed_max_create_fk(), 0);
    assert_eq!(load_seal(&runs_dir), 0);
    assert_eq!(q.scripthash_run_count(), 0);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The next Class A commit stores `header_txs` while the previous tip flush
/// can still be excluding stores. That put has to wait the flush out.
#[test]
fn class_a_header_store_waits_out_the_tip_flush() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-flush-wait");
    q.enter_direct_index_mode().unwrap();
    let (h0, ta0) = coinbase_block(0, Fk::NULL, None);
    let prev = q.connect_block(Height(0), &h0, &[ta0]).unwrap();
    let _commit = q.lock_tip_commit().unwrap();
    drop(_commit);
    let hold = q.hold_tip_flushes();
    let (h1, ta1) = coinbase_block(1, prev, Some(h0.hash));
    let prev = q.commit_class_a_only(&h1, &[ta1]).unwrap();
    q.flush_tip(Some(Height(0))).unwrap();
    let (h2, ta2) = coinbase_block(2, prev, Some(h1.hash));
    let h2_fk = thread::scope(|scope| {
        let put = scope.spawn(|| q.commit_class_a_only(&h2, &[ta2]));
        let blocked = q.wait_tip_commit_blocked(Duration::from_secs(2));
        hold.store(false, Ordering::SeqCst);
        let h2_fk = put
            .join()
            .unwrap()
            .expect("header range put waits for the tip flush");
        assert!(
            blocked,
            "the header put must block on the in-flight tip flush"
        );
        h2_fk
    });
    assert!(q.store.header_txs.has_body(h2_fk).unwrap());
    drop(q);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn force_rebuild_recollects_class_a_not_empty_materialize() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-force-recol");
    seed_direct_chain(&q, 6);
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.store.scripthash.has_durable_index());

    q.set_sh_force_rebuild(true);
    let result = q.finalize_sh_runs();
    let n_mat = result.expect("finalize after FORCE must not fail empty");
    assert!(
        n_mat > 0,
        "materialize must load Class A creates, got {n_mat}"
    );
    assert!(
        q.store.scripthash.has_durable_index(),
        "head must not stay empty after FORCE collect+pack"
    );
    assert!(q.store.scripthash.entry_count() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn unsorted_shards_finalize_skips_catalog_runs() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-unsorted");
    seed_direct_chain(&q, 5);
    let n_mat = q.finalize_sh_runs().expect("unsorted-shards finalize");
    assert!(
        n_mat > 0 || q.store.scripthash.has_durable_index(),
        "unsorted-shards must settle SH"
    );
    assert!(q.store.scripthash.has_durable_index());
    assert_eq!(
        q.scripthash_run_count(),
        0,
        "unsorted-shards must not leave catalog runs"
    );
    assert!(
        !rbitcoin_store::unsorted_shard_dir(q.store.path()).exists()
            || std::fs::read_dir(rbitcoin_store::unsorted_shard_dir(q.store.path()))
                .map(|it| it.count() == 0)
                .unwrap_or(true),
        "unsorted dir must be cleared after seal"
    );
    let sh = rbitcoin_store::script_hash(&[0x51, 0]);
    assert!(
        !q.scripthash_history(&sh).unwrap().is_empty(),
        "Class A creates must be queryable"
    );
    assert!(q.sh_is_tip_ready() || q.store.scripthash.entry_count() > 0);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_head_hwm_lag_discards_leftover_run() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-hwm-lag-skip");
    seed_direct_chain(&q, 6);
    let n0 = q.finalize_sh_runs().unwrap();
    assert!(n0 > 0 || q.store.scripthash.has_durable_index());
    assert!(q.store.scripthash.has_durable_index());
    let count_before = q.store.scripthash.entry_count();
    let tip_max = q.store.txs.count();
    assert!(tip_max >= 6);

    let lag = tip_max.saturating_sub(3).max(1);
    std::fs::write(
        dir.join(rbitcoin_store::INCLUDE_HWM_NAME),
        lag.to_le_bytes(),
    )
    .unwrap();
    assert!(
        q.store.scripthash.include_hwm() < tip_max,
        "planted HWM lag hwm={} tip_max={tip_max}",
        q.store.scripthash.include_hwm()
    );

    let runs_dir = dir.join("scripthash.runs");
    std::fs::create_dir_all(&runs_dir).unwrap();
    std::fs::write(runs_dir.join("000050.run"), b"leftover").unwrap();
    assert!(q.sh_run.on_disk_run_count() > 0);
    assert!(!q.sh_is_tip_ready(), "strict HWM/run check is still false");
    assert!(
        q.sh_use_writebehind(),
        "durable head must choose write-behind even when HWM lags"
    );

    let n1 = q.finalize_sh_runs().unwrap();
    assert_eq!(n1, 0, "must not collect onto a live head");
    assert_eq!(
        q.store.scripthash.entry_count(),
        count_before,
        "leftover run must not be applied onto the live head"
    );
    assert_eq!(q.scripthash_run_count(), 0, "leftover runs discarded");
    assert!(
        q.store.scripthash.include_hwm() >= tip_max,
        "Direct finalize backfills Class A tail (hwm={} tip_max={tip_max}), not leftover fk=99",
        q.store.scripthash.include_hwm()
    );
    assert!(
        q.store.scripthash.include_hwm() < 99,
        "leftover run fk=99 must not become include_hwm"
    );
    assert!(q.sh_is_tip_ready());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn finalize_sh_appends_unsorted_when_done_lags_before_any_shard() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-done-lag");
    seed_direct_chain(&q, 3);
    let n_shards = q.store.scripthash.head_shard_count();
    let udir = rbitcoin_store::unsorted_shard_dir(q.store.path());
    rbitcoin_store::collect_unsorted_shard_files(&q.store, &udir, n_shards, 1, None).unwrap();
    let done_last = rbitcoin_store::unsorted_done_last_fk(&udir, n_shards).unwrap();
    extend_direct_chain(&q, 2);
    assert!(q.store.txs.count() > done_last);
    assert!(
        !q.store.scripthash.unsealed_main_shards().is_empty(),
        "RAM merge writes head files; pack seal is later"
    );
    let _ = q.finalize_sh_runs().unwrap();
    let sh_old = rbitcoin_store::script_hash(&[0x51, 0]);
    let sh_new = rbitcoin_store::script_hash(&[0x51, 3]);
    assert!(
        !q.scripthash_history(&sh_old).unwrap().is_empty(),
        "creates from the original collect must remain"
    );
    assert!(
        !q.scripthash_history(&sh_new).unwrap().is_empty(),
        "creates after DONE must be in unsorted catch-up before pack"
    );
    assert!(q.sh_is_tip_ready());
    assert!(q.store.scripthash.include_hwm() >= q.store.txs.count());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn finalize_sh_backfills_class_a_tail_when_shards_already_sealed() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-shard-lag");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.store.scripthash.has_durable_index());
    assert!(q.sh_is_tip_ready());
    extend_direct_chain(&q, 2);
    assert!(
        !q.sh_is_tip_ready(),
        "new Class A after seal must lag honest include_hwm"
    );
    let _ = q.finalize_sh_runs().unwrap();
    let sh_new = rbitcoin_store::script_hash(&[0x51, 3]);
    assert!(
        !q.scripthash_history(&sh_new).unwrap().is_empty(),
        "Direct finalize must backfill Class A tail onto sealed shards"
    );
    assert!(q.sh_is_tip_ready());
    assert!(q.store.scripthash.include_hwm() >= q.store.txs.count());
    let _ = std::fs::remove_dir_all(&dir);
}

/// `submitblock` confirms while the tail backfill has already sampled
/// `txs.count()`. That create has to land in the inclusion watermark or
/// materialize refuses and Esplora never binds.
#[test]
fn backfill_includes_create_that_lands_after_count_sample() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-backfill-race");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.sh_is_tip_ready());
    extend_direct_chain(&q, 1);
    assert!(!q.sh_is_tip_ready());
    set_sh_backfill_after_sample_for_test(Some(extend_one_during_backfill));
    let result = q.backfill_sh_creates_from_class_a();
    set_sh_backfill_after_sample_for_test(None);
    result.expect("backfill");
    assert!(
        q.sh_is_tip_ready(),
        "a create confirmed after the count sample stays inside include_hwm"
    );
    assert!(q.store.scripthash.include_hwm() >= q.store.txs.count());
    let _ = std::fs::remove_dir_all(&dir);
}

fn extend_one_during_backfill(q: &Query) {
    extend_direct_chain(q, 1);
}

/// Two `put_create_batch_append` calls on one scripthash corrupt the chain
/// when they overlap. The Class A tail and `rbtc-sh-wb` both append, so the
/// tail has to wait on `sh.appender`.
#[test]
fn class_a_tail_backfill_waits_on_sh_appender() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-backfill-appender");
    let q = Arc::new(q);
    seed_direct_chain(&q, 3);
    q.finalize_sh_runs().unwrap();
    extend_direct_chain(&q, 1);
    assert!(!q.sh_is_tip_ready());
    let sh_new = rbitcoin_store::script_hash(&[0x51, 3]);
    assert!(
        q.store.scripthash.entries(&sh_new).unwrap().is_empty(),
        "the lagging create is not indexed yet"
    );

    let hold = q.sh.appender.lock().unwrap();
    let q2 = Arc::clone(&q);
    let (tx, rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let result = catch_unwind(AssertUnwindSafe(|| {
            q2.backfill_sh_creates_from_class_a().expect("backfill");
        }));
        let _ = tx.send(result);
    });
    let timed = rx.recv_timeout(Duration::from_millis(400));
    drop(hold);
    if timed.is_err() {
        rx.recv_timeout(Duration::from_secs(5))
            .expect("backfill finishes once sh.appender is free")
            .unwrap_or_else(|payload| std::panic::resume_unwind(payload));
    }
    worker.join().unwrap();
    match timed {
        Ok(Ok(())) => panic!("Class A tail backfill appended while sh.appender was held"),
        Ok(Err(payload)) => std::panic::resume_unwind(payload),
        Err(_) => {}
    }
    assert!(
        !q.store.scripthash.entries(&sh_new).unwrap().is_empty(),
        "the lagging create is indexed after the appender is released"
    );
    assert!(q.store.scripthash.include_hwm() >= q.store.txs.count());
    assert!(q.sh_is_tip_ready());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The ready sample inside the chase can miss a create that lands before
/// `finalize_sh_unsorted_cancellable` checks again. That create is still
/// behind the watermark, so the chase runs once more.
#[test]
fn finalize_chases_a_create_that_lands_after_the_ready_sample() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-ready-gap");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.sh_is_tip_ready());
    extend_direct_chain(&q, 1);
    assert!(!q.sh_is_tip_ready());
    set_sh_backfill_on_ready_for_test(Some(extend_one_during_backfill));
    let result = q.finalize_sh_unsorted_cancellable(None);
    set_sh_backfill_on_ready_for_test(None);
    result.expect("a create after the ready sample is chased");
    let sh_late = rbitcoin_store::script_hash(&[0x51, 4]);
    assert!(
        !q.scripthash_history(&sh_late).unwrap().is_empty(),
        "the create that landed after the ready sample is in the index"
    );
    assert!(q.sh_is_tip_ready());
    assert!(q.store.scripthash.include_hwm() >= q.store.txs.count());
    let _ = std::fs::remove_dir_all(&dir);
}

/// The tail backfill syncs the Class A creates it scans before
/// `include_hwm` names them. A failed sync leaves the watermark where it was
/// (and latches until restart).
#[test]
fn backfill_syncs_the_scanned_creates_before_the_watermark() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-backfill-sync");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    extend_direct_chain(&q, 1);
    // The Direct barrier also syncs Class A; let it finish before arming.
    q.join_tip_flush().unwrap();
    let before = q.store.scripthash.include_hwm();
    assert!(before < q.store.txs.count());
    rbitcoin_store::io_handle::arm_fail_sync(q.store.txs.txids.file.io_handle(), 1);
    assert!(
        q.backfill_sh_creates_from_class_a().is_err(),
        "the backfill published creates whose archive did not sync"
    );
    assert_eq!(q.store.scripthash.include_hwm(), before);
    let _ = std::fs::remove_dir_all(&dir);
}

fn in_flight_create(q: &rbitcoin_query::Query, salt: u8) {
    q.store
        .put_tx_full_batch_indexed(
            &[(
                rbitcoin_store::TxRecord {
                    txid: [salt; 32],
                    version: 1,
                    locktime: 0,
                    input_count: 1,
                    output_count: 1,
                },
                vec![rbitcoin_store::InputRecord::coinbase(
                    u32::MAX,
                    vec![0x01],
                    vec![],
                )],
                vec![rbitcoin_store::OutputRecord::unspent(50, vec![0x51, salt])],
            )],
            true,
        )
        .unwrap();
}

/// Confirm appends a block's creates before its range commits. The backfill
/// names that create in `include_hwm`, so a reopen keeps it: a later
/// transaction never takes a number the scripthash index already holds.
#[test]
fn reopen_keeps_a_create_the_backfill_names() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-backfill-mark");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    q.join_tip_flush().unwrap();
    in_flight_create(&q, 0xa1);
    let last = q.store.txs.count();
    q.backfill_sh_creates_from_class_a().unwrap();
    assert_eq!(q.store.scripthash.include_hwm(), last);
    let path = q.store.path().to_path_buf();
    drop(q);
    let q = rbitcoin_query::Query::open_or_create_tiny(&path).unwrap();
    assert_eq!(
        q.store.txs.count(),
        last,
        "open cut a create include_hwm names"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// An append publishes its bodies and locator before it writes `txid.body`,
/// `txstat`, and `input`. A backfill in that window names only finished
/// creates: a later open keeps them and cuts the unnamed one.
#[cfg(debug_assertions)]
#[test]
fn backfill_does_not_name_an_unfinished_append() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-backfill-unfinished");
    let q = Arc::new(q);
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    q.join_tip_flush().unwrap();
    in_flight_create(&q, 0xa3);
    let finished = q.store.txs.count();
    let inside = Arc::clone(&q);
    rbitcoin_store::tx_table::set_after_bodies_for_test(Some(Box::new(move || {
        assert_eq!(inside.store.txs.count(), finished + 1);
        inside.backfill_sh_creates_from_class_a().unwrap();
    })));
    in_flight_create(&q, 0xa4);
    rbitcoin_store::tx_table::set_after_bodies_for_test(None);
    assert_eq!(q.store.scripthash.include_hwm(), finished);
    let path = q.store.path().to_path_buf();
    drop(q);
    let q = rbitcoin_query::Query::open_or_create_tiny(&path).unwrap();
    assert_eq!(q.store.txs.count(), finished);
    let _ = std::fs::remove_dir_all(&dir);
}

/// After a failed append, `count` stays one past the settled creates: every
/// later append refuses on the stem count. Materialize compares readiness
/// against the settled count, so it returns instead of chasing a create no
/// pass can name. A shutdown joins this thread.
#[cfg(debug_assertions)]
#[test]
fn materialize_returns_after_a_failed_append() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-failed-append");
    let q = Arc::new(q);
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    q.join_tip_flush().unwrap();
    in_flight_create(&q, 0xa5);
    let finished = q.store.txs.count();
    let txids = q.store.txs.txids.file.io_handle();
    rbitcoin_store::io_handle::arm_short_pwrite(txids);
    let failed = q.store.put_tx_full_batch_indexed(
        &[(
            rbitcoin_store::TxRecord {
                txid: [0xa6; 32],
                version: 1,
                locktime: 0,
                input_count: 1,
                output_count: 1,
            },
            vec![rbitcoin_store::InputRecord::coinbase(
                u32::MAX,
                vec![0x01],
                vec![],
            )],
            vec![rbitcoin_store::OutputRecord::unspent(50, vec![0x51, 0xa6])],
        )],
        true,
    );
    rbitcoin_store::io_handle::disarm_pwrite(txids);
    assert!(failed.is_err());
    assert_eq!(q.store.txs.count(), finished + 1);
    let q2 = Arc::clone(&q);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = tx.send(q2.finalize_sh_unsorted_cancellable(None).is_ok());
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("materialize did not return after a failed append");
    assert!(q.store.scripthash.include_hwm() <= finished);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A datadir from a binary without `class_a.synced`, after a clean
/// shutdown. Every create has a block range, so the floor is the last range
/// and open cuts nothing: the address index, `include_hwm`, and `tx.head`
/// all stay as they were, with no rebuild.
#[test]
fn clean_datadir_without_the_synced_mark_opens_whole() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-upgrade-no-synced");
    seed_direct_chain(&q, 4);
    let _ = q.finalize_sh_runs().unwrap();
    q.join_tip_flush().unwrap();
    q.flush_for_shutdown().unwrap();
    let count = q.store.txs.count();
    let include = q.store.scripthash.include_hwm();
    assert_eq!(include, count);
    let head = q.store.txs.head.last_inserted_fk();
    let path = q.store.path().to_path_buf();
    drop(q);
    let _ = std::fs::remove_file(path.join(rbitcoin_store::tx_table::CLASS_A_SYNCED_NAME));

    rbitcoin_log::capture_logs(true);
    let q = rbitcoin_query::Query::open_or_create_tiny(&path).unwrap();
    let logs = rbitcoin_log::take_logs();
    rbitcoin_log::capture_logs(false);
    assert_eq!(
        q.store.txs.count(),
        count,
        "open cut a create every range names"
    );
    assert_eq!(q.store.scripthash.include_hwm(), include);
    assert_eq!(q.store.txs.head.last_inserted_fk(), head);
    assert!(
        !logs
            .iter()
            .any(|(_, l)| l.contains("tx.head rebuild") || l.contains("tx.head leads")),
        "open rebuilt tx.head: {logs:?}"
    );
    assert!(q.sh_is_tip_ready());
    let _ = std::fs::remove_dir_all(&dir);
}

/// An older binary's backfill named a create whose block range never
/// committed, then the process died. That datadir has no `class_a.synced`,
/// so open cuts the create while `include_hwm` still names it. The index is
/// past the archive: the next finalize deletes it and recollects from Class
/// A, so the cut create's posting cannot attach to the create that takes its
/// number.
#[test]
fn index_past_the_archive_is_recollected() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-past-archive");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    q.join_tip_flush().unwrap();
    in_flight_create(&q, 0xb1);
    let last = q.store.txs.count();
    q.backfill_sh_creates_from_class_a().unwrap();
    assert_eq!(q.store.scripthash.include_hwm(), last);
    let path = q.store.path().to_path_buf();
    drop(q);
    let _ = std::fs::remove_file(path.join(rbitcoin_store::tx_table::CLASS_A_SYNCED_NAME));

    let q = rbitcoin_query::Query::open_or_create_tiny(&path).unwrap();
    assert_eq!(
        q.store.txs.count(),
        last - 1,
        "open cut the unranged create"
    );
    let _ = q.finalize_sh_runs().unwrap();
    assert!(
        q.store.scripthash.include_hwm() <= q.store.txs.count(),
        "the index still names a create the archive cut"
    );
    let cut = rbitcoin_store::script_hash(&[0x51, 0xb1]);
    assert!(q.scripthash_history(&cut).unwrap_or_default().is_empty());
    assert!(q.sh_is_tip_ready());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Collect the unsorted keys, and with `posts` the postings, through the
/// whole archive, then reopen as after an older binary: no synced mark, so
/// open cuts every create no block range committed.
fn collect_then_reopen_without_the_synced_mark(q: Query, posts: bool) -> Query {
    use rbitcoin_store::scripthash_materialize as sm;
    let usd = sm::unsorted_shard_dir(q.store.path());
    std::fs::create_dir_all(&usd).unwrap();
    let n_shards = q.store.scripthash.head_shard_count();
    let keys =
        sm::collect_unsorted_covering_class_a(&q.store, &usd, n_shards, 1, false, None).unwrap();
    assert_eq!(keys.last_fk, q.store.txs.count());
    if posts {
        sm::collect_posts_covering(&q.store.txs, &q.store.scripthash, &usd, 1, false, None)
            .unwrap();
    }
    let path = q.store.path().to_path_buf();
    drop(q);
    let _ = std::fs::remove_file(path.join(rbitcoin_store::tx_table::CLASS_A_SYNCED_NAME));
    Query::open_or_create_tiny(&path).unwrap()
}

/// An older binary collected keys through a create no block range named, then
/// stopped before the pack. Open cuts that create and the next block reuses
/// its number. Start must drop the collect before Direct catchup appends:
/// resuming it would skip the new create, and its confirmed output would
/// never be indexed.
#[test]
fn collect_past_the_archive_is_recollected() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-collect-past");
    seed_direct_chain(&q, 3);
    q.join_tip_flush().unwrap();
    q.store.flush_class_c_tip().unwrap();
    in_flight_create(&q, 0xb2);
    let last = q.store.txs.count();
    let q = collect_then_reopen_without_the_synced_mark(q, false);
    assert_eq!(
        q.store.txs.count(),
        last - 1,
        "open cut the unranged create"
    );
    // Node start: the guard runs before the index mode is chosen.
    q.discard_unproven_scripthash_head().unwrap();
    q.enter_direct_index_mode().unwrap();
    extend_direct_chain(&q, 1);
    assert_eq!(
        q.store.txs.count(),
        last,
        "the next block reused the number"
    );
    let reused = rbitcoin_store::script_hash(&[0x51, 3]);
    assert!(
        q.scripthash_history(&reused).unwrap_or_default().is_empty(),
        "Direct catchup indexes nothing before finalize"
    );
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.sh_is_tip_ready());
    let path = q.store.path().to_path_buf();
    drop(q);
    let q = Query::open_or_create_tiny(&path).unwrap();
    assert!(
        !q.scripthash_history(&reused).unwrap_or_default().is_empty(),
        "the confirmed output on the reused create is missing"
    );
    let cut = rbitcoin_store::script_hash(&[0x51, 0xb2]);
    assert!(q.scripthash_history(&cut).unwrap_or_default().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Collect marks an older binary left past the archive must not reach
/// `include_hwm` or SEAL on the next finalize.
#[test]
fn collect_marks_past_the_archive_do_not_publish() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-done-past");
    seed_direct_chain(&q, 3);
    q.join_tip_flush().unwrap();
    in_flight_create(&q, 0xb3);
    let q = collect_then_reopen_without_the_synced_mark(q, true);
    let _ = q.finalize_sh_runs().unwrap();
    let count = q.store.txs.count();
    assert!(
        q.store.scripthash.include_hwm() <= count,
        "include_hwm names a create the archive cut"
    );
    assert!(
        q.sh_run.sealed_max_create_fk() <= count,
        "SEAL names a create the archive cut"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A rebuild mark found at open drops the old index files there. The
/// finalize after it recollects Class A and drops the mark.
#[test]
fn rebuild_mark_at_open_recollects_then_clears() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-rebuild-open");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    let before = q.store.scripthash.entry_count();
    assert!(before > 0);
    rbitcoin_store::scripthash::mark_sh_rebuild(q.store.path()).unwrap();
    drop(q);
    let q = rbitcoin_query::Query::open_or_create_tiny(dir.path()).unwrap();
    assert!(q.sh_force_rebuild());
    let _ = q.finalize_sh_runs().unwrap();
    assert!(!rbitcoin_store::scripthash::sh_rebuild_marked(
        q.store.path()
    ));
    assert!(q.sh_is_tip_ready());
    assert_eq!(
        q.store.scripthash.entry_count(),
        before,
        "the rebuild after open recollected Class A"
    );
}

/// A rebuild mark wipes and recollects the index on the next finalize, then
/// is dropped so the start after that keeps the head.
#[test]
fn rebuild_mark_recollects_then_clears() {
    let (dir, q) = rbitcoin_query::testutil::tiny_query_labeled("q-sh-rebuild-mark");
    seed_direct_chain(&q, 3);
    let _ = q.finalize_sh_runs().unwrap();
    assert!(q.sh_is_tip_ready());
    rbitcoin_store::scripthash::mark_sh_rebuild(q.store.path()).unwrap();
    assert!(q.sh_force_rebuild());
    let _ = q.finalize_sh_runs().unwrap();
    assert!(!rbitcoin_store::scripthash::sh_rebuild_marked(
        q.store.path()
    ));
    assert!(!q.sh_force_rebuild());
    assert!(q.sh_is_tip_ready());
    assert!(
        q.store.scripthash.entry_count() > 0,
        "the rebuild recollected Class A"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
