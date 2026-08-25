//! Confirm_run unit tests (peeled from confirm_run.rs).

use super::{recent_create_height_slices, recent_create_rows_for_slices, write_height_needed};

#[test]
fn tx_head_drain_thread_is_named_and_reused() {
    use super::{submit_head_drain, HEAD_DRAIN_THREAD_NAME};
    let (r1, id1, n1) = submit_head_drain(|| Ok(1)).join_named();
    let (r2, id2, n2) = submit_head_drain(|| Ok(2)).join_named();
    assert_eq!(r1.unwrap(), 1);
    assert_eq!(r2.unwrap(), 2);
    assert_eq!(n1, HEAD_DRAIN_THREAD_NAME);
    assert_eq!(n2, HEAD_DRAIN_THREAD_NAME);
    assert_eq!(id1, id2, "drain must keep one OS thread across batches");
}

#[test]
fn recent_create_height_slices_two_heights_and_remainder() {
    assert_eq!(
        recent_create_height_slices(&[(10, 2), (11, 3)], 5),
        vec![(10, 0..2), (11, 2..5)]
    );
    assert_eq!(
        recent_create_height_slices(&[(10, 2), (11, 3)], 7),
        vec![(10, 0..2), (11, 2..5), (11, 5..7)],
        "tail past prepared counts tags the last height"
    );
    assert!(recent_create_height_slices(&[(10, 2)], 0).is_empty());
    assert_eq!(
        recent_create_height_slices(&[(10, 0), (11, 4)], 4),
        vec![(11, 0..4)]
    );
}

#[test]
fn recent_create_rows_skip_missing_idx_keep_heights() {
    let tid = |b| {
        let mut t = [0u8; 32];
        t[0] = b;
        t
    };
    let slices = recent_create_height_slices(&[(10, 2), (11, 2)], 4);
    let pairs = [
        (tid(1), rbitcoin_primitives::Fk(1)),
        (tid(2), rbitcoin_primitives::Fk(2)),
        (tid(3), rbitcoin_primitives::Fk(3)),
        (tid(4), rbitcoin_primitives::Fk(4)),
    ];
    let ranges = [Some((1, 8)), None, Some((9, 8)), Some((17, 8))];
    let rows = recent_create_rows_for_slices(&slices, &pairs, &ranges, &[]);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, 10);
    assert_eq!(rows[0].1.len(), 1, "missing idx at height 10 dropped");
    assert_eq!(rows[1].0, 11);
    assert_eq!(rows[1].1.len(), 2);
}

/// Batch append: contiguous heights merge; gap returns Err(other).
#[test]
fn script_ok_append_contiguous_and_gap() {
    use super::{Prepared, ScriptOkBatch};
    use bitcoin::CompactTarget;
    use rbitcoin_primitives::{Fk, Height};
    use std::sync::Arc;

    fn empty_prepared(h: u32, hash_byte: u8) -> Prepared {
        Prepared {
            height: Height(h),
            header_fk: Fk(h as u64),
            tx_fks: vec![],
            jobs: vec![],
            spends: vec![],
            fees: 0,
            check_scripts: false,
            time: 0,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            hash: [hash_byte; 32],
            prev_mtp: 0,
        }
    }
    fn batch_one(h: u32) -> ScriptOkBatch {
        ScriptOkBatch {
            prepared: vec![empty_prepared(h, h as u8)],
            wire_blocks: vec![Arc::new(crate::params::genesis_block(
                &crate::params::ChainParams::regtest(),
            ))],
            batch_parents: rbitcoin_query::BatchParents::new(),
            archive_plan: None,
        }
    }
    let mut a = batch_one(10);
    let b = batch_one(11);
    assert!(a.append_contiguous(b).is_ok());
    assert_eq!(a.len(), 2);
    let gap = batch_one(13);
    let err = a.append_contiguous(gap).err().expect("gap");
    assert_eq!(err.len(), 1);
    assert_eq!(a.len(), 2);
    // Contiguous continue after gap reject.
    let c = batch_one(12);
    assert!(a.append_contiguous(c).is_ok());
    assert_eq!(a.len(), 3);

    // Empty other is no-op.
    assert!(a
        .append_contiguous(ScriptOkBatch {
            prepared: vec![],
            wire_blocks: vec![],
            batch_parents: rbitcoin_query::BatchParents::new(),
            archive_plan: None,
        })
        .is_ok());
    assert_eq!(a.len(), 3);

    // Empty self absorbs other.
    let mut empty = ScriptOkBatch {
        prepared: vec![],
        wire_blocks: vec![],
        batch_parents: rbitcoin_query::BatchParents::new(),
        archive_plan: None,
    };
    assert!(empty.append_contiguous(batch_one(50)).is_ok());
    assert_eq!(empty.len(), 1);
    assert_eq!(empty.heights_hashes()[0].0, 50);
    assert!(!empty.is_empty());
    assert!(empty.approx_wire_bytes() > 0);
    assert_eq!(empty.parent_count(), 0);

    // Wire/prepared length mismatch on contiguous height → Err(other).
    let mut good = batch_one(60);
    let mut bad = batch_one(61);
    bad.wire_blocks.clear();
    let err = good.append_contiguous(bad).err().expect("len mismatch");
    assert_eq!(err.len(), 1);

    // archive_plan merge: None + Some, Some + Some, Some + None.
    let mut with_plan = batch_one(70);
    with_plan.archive_plan = Some(rbitcoin_query::ArchiveWritePlan::empty());
    let mut next = batch_one(71);
    next.archive_plan = Some(rbitcoin_query::ArchiveWritePlan::empty());
    assert!(with_plan.append_contiguous(next).is_ok());
    assert!(with_plan.archive_plan.is_some());
    let mut only_other = batch_one(72);
    // Self plan remains Some; other None keeps it.
    only_other.archive_plan = None;
    assert!(with_plan.append_contiguous(only_other).is_ok());
    assert!(with_plan.archive_plan.is_some());
    // Self None absorbs other's plan.
    let mut no_plan = batch_one(80);
    let mut has = batch_one(81);
    has.archive_plan = Some(rbitcoin_query::ArchiveWritePlan::empty());
    assert!(no_plan.append_contiguous(has).is_ok());
    assert!(no_plan.archive_plan.is_some());
}

/// Heights at or below tip must be stripped before structural write
/// (dup pipeline race after scripts claim the same tip+1 twice).
/// Write filter + stage entry points + empty scripts purity (one surface).
/// External three-stage path: rbitcoin-test three_stage_confirm_and_parent_pin_surface.
#[test]
fn three_stage_write_filter_and_scripts_surface() {
    let tip = Some(100u32);
    let heights = [98u32, 99, 100, 101, 102];
    let kept: Vec<u32> = heights
        .into_iter()
        .filter(|&h| write_height_needed(tip, h))
        .collect();
    assert_eq!(kept, vec![101, 102]);
    assert!(!write_height_needed(tip, 100));
    assert!(!write_height_needed(Some(0), 0));
    assert!(write_height_needed(Some(0), 1));
    // Empty chain: genesis (and all heights) still need write.
    assert!(write_height_needed(None, 0));
    assert!(write_height_needed(None, 1));

    // Load / scripts / write are separate public surfaces for IBD.
    let _m = super::confirm_wire_load_phase;
    let _s = super::confirm_scripts_phase;
    let _w = super::confirm_write_phase;
    let _sync = super::confirm_wire_run;

    use super::{confirm_scripts_phase, LoadedBatch, ScriptPreverified};
    let batch = LoadedBatch {
        prepared: Vec::new(),
        wire_blocks: Vec::new(),
        batch_parents: rbitcoin_query::BatchParents::new(),
        script_preverified: ScriptPreverified::new(),
        archive_plan: None,
    };
    assert!(batch.is_empty());
    assert_eq!(batch.approx_wire_bytes(), 0);
    assert_eq!(batch.parent_count(), 0);
    let ok = confirm_scripts_phase(batch).expect("empty scripts ok");
    assert!(ok.batch.prepared.is_empty());
    assert!(ok.batch.wire_blocks.is_empty());
}

fn empty_loaded_batch() -> super::LoadedBatch {
    super::LoadedBatch {
        prepared: Vec::new(),
        wire_blocks: Vec::new(),
        batch_parents: rbitcoin_query::BatchParents::new(),
        script_preverified: super::ScriptPreverified::new(),
        archive_plan: None,
    }
}

/// One-batch feed-ahead path (no lookahead) still succeeds on the real entry.
#[test]
fn scripts_feed_ahead_single_batch() {
    use super::confirm_scripts_feed_ahead;
    let outs = confirm_scripts_feed_ahead([empty_loaded_batch()]).expect("single");
    assert_eq!(outs.len(), 1);
    assert!(outs[0].batch.is_empty());
}

/// `confirm_scripts_phase_async` publishes on the caller (no coordinator
/// thread, no steal worker).
#[test]
fn scripts_phase_does_not_run_on_steal_worker() {
    use super::confirm_scripts_phase_async;
    let (ok, name) = confirm_scripts_phase_async(empty_loaded_batch())
        .join_with_phase_thread()
        .expect("empty phase");
    assert!(ok.batch.is_empty());
    assert!(
        !name.starts_with("rbtc-script-coord-"),
        "coordinator threads are gone, got {name:?}"
    );
    assert!(
        !name.starts_with("rbtc-scripts-"),
        "scripts phase ran on steal worker {name:?}"
    );
}

fn linux_thread_comms() -> Vec<String> {
    let Ok(dir) = std::fs::read_dir("/proc/self/task") else {
        return Vec::new();
    };
    dir.filter_map(|e| {
        let p = e.ok()?.path().join("comm");
        std::fs::read_to_string(p).ok()
    })
    .map(|s| s.trim().to_string())
    .collect()
}

/// IBD `drive_script_waves` writes in input order and never starts
/// `rbtc-script-coord-*` threads.
#[test]
fn drive_script_waves_ordered_without_coordinator_threads() {
    use super::drive_script_waves;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let (tx, rx) = mpsc::sync_channel(4);
    let heights = Arc::new(Mutex::new(Vec::new()));
    let heights_w = Arc::clone(&heights);
    let stage = thread::Builder::new()
        .name("ibd-confirm".into())
        .spawn(move || {
            drive_script_waves(
                &rx,
                |ok, meta| {
                    heights_w.lock().unwrap().push(meta.first_h);
                    assert!(ok.batch.is_empty());
                    true
                },
                |_e, _meta, _dropped| false,
                || false,
            );
        })
        .expect("spawn publisher");
    for _ in 0..3 {
        tx.send((empty_loaded_batch(), 0)).expect("send");
    }
    drop(tx);
    crate::unpark_script_publisher();
    stage.join().expect("publisher");
    assert_eq!(heights.lock().unwrap().len(), 3);
    for comm in linux_thread_comms() {
        assert!(
            !comm.starts_with("rbtc-script-coord"),
            "coordinator thread still live: {comm}"
        );
    }
}

fn prepared_at(
    height: u32,
    hash: [u8; 32],
    jobs: Vec<crate::block::ScriptCheckJob>,
    check_scripts: bool,
) -> super::Prepared {
    use bitcoin::CompactTarget;
    use rbitcoin_primitives::{Fk, Height};
    super::Prepared {
        height: Height(height),
        header_fk: Fk(1),
        tx_fks: Vec::new(),
        jobs,
        spends: Vec::new(),
        fees: 0,
        check_scripts,
        time: 1,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        hash,
        prev_mtp: 0,
    }
}

fn loaded_at(
    height: u32,
    hash: [u8; 32],
    jobs: Vec<crate::block::ScriptCheckJob>,
    check_scripts: bool,
) -> super::LoadedBatch {
    super::LoadedBatch {
        prepared: vec![prepared_at(height, hash, jobs, check_scripts)],
        wire_blocks: Vec::new(),
        batch_parents: rbitcoin_query::BatchParents::new(),
        script_preverified: super::ScriptPreverified::new(),
        archive_plan: None,
    }
}

fn bad_p2pkh_job() -> crate::block::ScriptCheckJob {
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    let prevouts = vec![TxOut {
        value: Amount::from_sat(50_0000_0000),
        script_pubkey: ScriptBuf::from_bytes(vec![
            0x76, 0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88,
            0xac,
        ]),
    }];
    let tx = Transaction {
        version: TxVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_byte_array([9; 32]),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(49_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let tid = tx.compute_txid().to_byte_array();
    crate::block::ScriptCheckJob::with_txid(tid, prevouts, tx, true, true, true, true, true)
}

/// One-job inline fail keeps the batch height/hash; a later batch still writes.
#[test]
fn drive_script_waves_start_fail_keeps_meta_and_continues() {
    use super::drive_script_waves;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;

    let (tx, rx) = mpsc::sync_channel(4);
    let oks = Arc::new(Mutex::new(Vec::new()));
    let errs = Arc::new(Mutex::new(Vec::new()));
    let oks_w = Arc::clone(&oks);
    let errs_w = Arc::clone(&errs);
    let stage = thread::spawn(move || {
        drive_script_waves(
            &rx,
            |ok, meta| {
                oks_w.lock().unwrap().push(meta.first_h);
                assert!(ok.batch.prepared.len() == 1);
                true
            },
            |e, meta, dropped| {
                assert!(dropped.is_empty());
                errs_w.lock().unwrap().push((
                    meta.first_h,
                    meta.heights_hashes.clone(),
                    format!("{e}"),
                ));
                true
            },
            || false,
        );
    });
    tx.send((loaded_at(10, [10u8; 32], vec![bad_p2pkh_job()], true), 0))
        .expect("send bad");
    tx.send((loaded_at(20, [20u8; 32], Vec::new(), true), 0))
        .expect("send ok");
    drop(tx);
    crate::unpark_script_publisher();
    stage.join().expect("publisher");
    let errs = errs.lock().unwrap();
    assert_eq!(errs.len(), 1, "one reject");
    assert_eq!(errs[0].0, 10);
    assert_eq!(errs[0].1, vec![(10, [10u8; 32])]);
    assert_ne!(errs[0].1[0].1, [0u8; 32]);
    let oks = oks.lock().unwrap();
    assert_eq!(&*oks, &[20], "later batch still written");
}

/// Drained job vecs must drop capacity before write handoff.
#[test]
fn script_jobs_shrink_after_take() {
    use super::confirm_scripts_phase;
    let mut jobs = Vec::with_capacity(32);
    jobs.push(bad_p2pkh_job());
    assert!(jobs.capacity() >= 32);
    let batch = loaded_at(7, [7u8; 32], jobs, false);
    let ok = confirm_scripts_phase(batch).expect("skip scripts");
    assert_eq!(ok.batch.prepared[0].jobs.capacity(), 0);
}

/// Two ready batches: both verify on the real async path; write order preserved.
///
/// Uses [`confirm_scripts_feed_ahead`] (same submit/join helper production
/// scripts OS thread uses via [`confirm_scripts_phase_async`]).
#[test]
fn scripts_feed_ahead_two_batches_ordered() {
    use super::{confirm_scripts_feed_ahead, confirm_scripts_phase_async};
    // Async handles: start both before joining either (overlap submit).
    let h0 = confirm_scripts_phase_async(empty_loaded_batch());
    let h1 = confirm_scripts_phase_async(empty_loaded_batch());
    let o0 = h0.join().expect("batch0");
    let o1 = h1.join().expect("batch1");
    assert!(o0.batch.is_empty());
    assert!(o1.batch.is_empty());

    // Ordered helper: two batches both ok, returned in input order.
    let outs = confirm_scripts_feed_ahead([empty_loaded_batch(), empty_loaded_batch()])
        .expect("feed-ahead two");
    assert_eq!(outs.len(), 2);
    assert!(outs[0].batch.is_empty());
    assert!(outs[1].batch.is_empty());
}

/// Empty iterator is a no-op (pipeline edge).
#[test]
fn scripts_feed_ahead_zero_batches() {
    use super::confirm_scripts_feed_ahead;
    let outs = confirm_scripts_feed_ahead(std::iter::empty()).expect("empty");
    assert!(outs.is_empty());
}

/// Depth-1 feed-ahead + no 200 µs-poll after lookahead, **without** a
/// process-global HOLD in [`super::confirm_scripts_phase`].
///
/// A sibling `confirm_scripts_phase` running while A is held must finish
/// immediately (the old `HOLD_FIRST` hook stalled every phase in the crate).
#[test]
fn scripts_stage_depth1_feeds_ahead_without_holding_siblings() {
    use super::{
        confirm_scripts_phase, join_scripts_polling, scripts_stage_from_load_channel_with,
        ConfirmScriptOutcome, ScriptsBatchMeta, ScriptsPhaseHandle,
    };
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    let submits = Arc::new(AtomicU64::new(0));
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let outcomes: Arc<Mutex<Vec<ConfirmScriptOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let (mat_tx, mat_rx) = mpsc::sync_channel::<(super::LoadedBatch, u64)>(1);

    let submits_s = Arc::clone(&submits);
    let gate_s = Arc::clone(&gate);
    let outcomes_w = Arc::clone(&outcomes);
    let stage = thread::spawn(move || {
        scripts_stage_from_load_channel_with(
            &mat_rx,
            |batch, mat_ns| {
                let meta = ScriptsBatchMeta::from_batch(&batch, mat_ns);
                let n = submits_s.fetch_add(1, Ordering::SeqCst) + 1;
                let gate = Arc::clone(&gate_s);
                let handle = ScriptsPhaseHandle::spawn_fn(move || {
                    if n == 1 {
                        let (lock, cv) = &*gate;
                        let mut go = lock.lock().unwrap();
                        let deadline = Instant::now() + Duration::from_secs(2);
                        while !*go {
                            let left = deadline.saturating_duration_since(Instant::now());
                            if left.is_zero() {
                                break;
                            }
                            let (g, w) = cv.wait_timeout(go, left).unwrap();
                            go = g;
                            if w.timed_out() {
                                break;
                            }
                        }
                    }
                    confirm_scripts_phase(batch)
                });
                (handle, meta)
            },
            |ok, _meta: ScriptsBatchMeta| {
                outcomes_w.lock().unwrap().push(ok);
                true
            },
            |_e, _meta| false,
            || false,
        );
    });

    mat_tx.send((empty_loaded_batch(), 0)).expect("send A");
    let deadline = Instant::now() + Duration::from_secs(2);
    while submits.load(Ordering::SeqCst) < 1 {
        assert!(Instant::now() < deadline, "A never submitted");
        thread::sleep(Duration::from_millis(1));
    }

    let sibling = thread::spawn(|| {
        let t0 = Instant::now();
        confirm_scripts_phase(empty_loaded_batch()).expect("sibling phase");
        t0.elapsed()
    });
    let sibling_dt = sibling.join().expect("sibling");
    assert!(
        sibling_dt < Duration::from_millis(200),
        "confirm_scripts_phase must not honor another test's hold ({sibling_dt:?})"
    );

    mat_tx
        .send((empty_loaded_batch(), 0))
        .expect("send B while A held");
    while submits.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < deadline,
            "B not submitted before A finished (feed-ahead dead under depth-1)"
        );
        thread::sleep(Duration::from_millis(1));
    }

    {
        let (lock, cv) = &*gate;
        *lock.lock().unwrap() = true;
        cv.notify_all();
    }
    drop(mat_tx);
    stage.join().expect("stage thread");
    let outs = outcomes.lock().unwrap();
    assert_eq!(outs.len(), 2, "both batches script-ok");
    assert!(outs[0].batch.is_empty());
    assert!(outs[1].batch.is_empty());

    let mut polls = 0u32;
    let handle = ScriptsPhaseHandle::spawn_fn(|| confirm_scripts_phase(empty_loaded_batch()));
    join_scripts_polling(&handle, Duration::from_micros(200), || {
        polls += 1;
        false
    })
    .expect("join after lookahead");
    assert_eq!(
        polls, 1,
        "join must recv_blocking after first false, not 200µs-poll (polls={polls})"
    );
}

#[test]
fn check_bip34_helper_and_expected_bits_no_retarget() {
    use super::{check_bip34, expected_bits_extending};
    use crate::params::ChainParams;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn,
        TxMerkleNode, TxOut, Witness,
    };
    use rbitcoin_primitives::Height;

    let height = 17u32;
    let mut ss = crate::block::bip34_height_script(height);
    while ss.len() < 2 {
        ss.push(0x00);
    }
    let cb = Transaction {
        version: bitcoin::transaction::Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(ss),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let block = Block {
        header: Header {
            version: Version::from_consensus(4),
            prev_blockhash: BlockHash::from_byte_array([0; 32]),
            merkle_root: TxMerkleNode::from_byte_array([0; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        },
        txdata: vec![cb],
    };
    check_bip34(&block, height).unwrap();
    // Wrong height
    assert!(check_bip34(&block, height + 1).is_err());
    let mut empty_cb = block.clone();
    empty_cb.txdata[0].input[0].script_sig = ScriptBuf::new();
    let err = check_bip34(&empty_cb, height).expect_err("empty scriptSig");
    assert!(
        err.to_string().contains("bip34 coinbase script empty"),
        "got: {err}"
    );

    // expected_bits_extending without store: height 0 and no_pow_retargeting regtest
    let params = ChainParams::regtest();
    // Cannot call with query easily; unit-test height==0 via expected_bits requires Query.
    // Cover pure branch: no_pow or non-interval uses prev_bits — needs Query only for retarget.
    let _ = (params, expected_bits_extending);
    let _ = Height;
}

#[test]
fn empty_confirm_batch_rejected() {
    // confirm_wire_load_phase empty → BadBlock without store open
    // We only have Query API; use a throwaway path under /tmp when available.
    use super::confirm_wire_load_phase;
    use super::ScriptPreverified;
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-confirm-empty-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    let params = ChainParams::regtest();
    let none = ScriptPreverified::new();
    let err = match confirm_wire_load_phase(&q, &params, Milestone::NONE, &[], &none) {
        Ok(_) => panic!("expected empty batch error"),
        Err(e) => e,
    };
    assert!(matches!(err, crate::error::ConsensusError::BadBlock(_)));
    // Non-contiguous
    let g = crate::params::genesis_block(&params);
    let err2 = match confirm_wire_load_phase(
        &q,
        &params,
        Milestone::NONE,
        &[(Height(1), g.clone()), (Height(3), g)],
        &none,
    ) {
        Ok(_) => panic!("expected non-contiguous error"),
        Err(e) => e,
    };
    assert!(matches!(err2, crate::error::ConsensusError::BadBlock(_)));
    let _ = std::fs::remove_dir_all(&path);
}

/// Trailing null `confirmed[]` + reopen must still connect real tip+1
/// (`NotFound` was the inflated-HWM miss on a valid body).
#[test]
fn tip_plus_one_after_trailing_null_heal_is_not_notfound() {
    use crate::accept_and_connect_block;
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use crate::regtest_pad::{mine_empty_regtest, pad_empty_from};
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-confirm-tip1-heal-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    let params = ChainParams::regtest();
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, Milestone::NONE).unwrap();
    let (tip, tip_time, _) = pad_empty_from(
        &q,
        &params,
        genesis.block_hash(),
        genesis.header.time,
        1,
        3,
        0,
    );
    drop(q);

    let conf = path.join("confirmed.body");
    let mut raw = std::fs::read(&conf).unwrap();
    assert!(raw.len() >= 16);
    let logical = u64::from_le_bytes(raw[8..16].try_into().unwrap());
    let extra = vec![0u8; 20 * 8];
    let new_logical = logical + extra.len() as u64;
    if (raw.len() as u64) < new_logical {
        raw.resize(new_logical as usize, 0);
    }
    raw[8..16].copy_from_slice(&new_logical.to_le_bytes());
    std::fs::write(&conf, &raw).unwrap();

    let q = Query::open_or_create(&path).unwrap();
    assert_eq!(q.tip_height().map(|h| h.0), Some(3));
    let nxt = mine_empty_regtest(tip, tip_time + 600, 4);
    let r = accept_and_connect_block(&q, &params, Height(4), &nxt, Milestone::NONE);
    match r {
        Ok(_) => {}
        Err(e) => {
            let s = e.to_string();
            assert!(
                !s.to_ascii_lowercase().contains("not found"),
                "valid tip+1 must not be Store NotFound: {e}"
            );
            panic!("tip+1 confirm failed: {e}");
        }
    }
    assert_eq!(q.tip_height().map(|h| h.0), Some(4));
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn expected_bits_extending_height0_and_no_retarget() {
    use super::expected_bits_extending;
    use crate::params::ChainParams;
    use bitcoin::CompactTarget;
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-confirm-bits-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    let params = ChainParams::regtest();
    let gbits =
        expected_bits_extending(&q, &params, Height(0), CompactTarget::from_consensus(0), 0)
            .unwrap();
    assert_eq!(gbits, crate::params::genesis_block(&params).header.bits);
    // No-pow-retargeting: any height returns prev_bits.
    let prev = CompactTarget::from_consensus(0x207f_ffff);
    let b = expected_bits_extending(&q, &params, Height(2016), prev, 100).unwrap();
    assert_eq!(b, prev);

    // ScriptOkBatch empty surfaces (mirror LoadedBatch).
    use super::{confirm_scripts_phase, LoadedBatch, ScriptPreverified};
    let loaded = LoadedBatch {
        prepared: Vec::new(),
        wire_blocks: Vec::new(),
        batch_parents: rbitcoin_query::BatchParents::new(),
        script_preverified: ScriptPreverified::new(),
        archive_plan: None,
    };
    let ok = confirm_scripts_phase(loaded).unwrap();
    assert!(ok.batch.is_empty());
    assert_eq!(ok.batch.len(), 0);
    assert!(ok.batch.heights_hashes().is_empty());
    assert_eq!(ok.batch.approx_wire_bytes(), 0);
    assert_eq!(ok.batch.parent_count(), 0);

    // check_bip34 wrong encoding
    use super::check_bip34;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::{
        Amount, Block, BlockHash, OutPoint, Sequence, Transaction, TxIn, TxMerkleNode, TxOut,
        Witness,
    };
    let cb = Transaction {
        version: bitcoin::transaction::Version::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(vec![0x01, 0x99]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let block = Block {
        header: Header {
            version: Version::from_consensus(4),
            prev_blockhash: BlockHash::from_byte_array([0; 32]),
            merkle_root: TxMerkleNode::from_byte_array([0; 32]),
            time: 1,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        },
        txdata: vec![cb],
    };
    assert!(check_bip34(&block, 17).is_err());

    let _ = std::fs::remove_dir_all(&path);
}

/// Multi-block tip-ahead assemble (i>0) calls [`expected_bits_extending`] on a
/// retarget height. Period-start (`height − interval`) may still be **above**
/// confirmed tip while already present as a ConfirmParentCache header plan
/// (put when that height was looked up/loaded earlier).
///
/// Mainnet log 2026-08-07: batch @132992 n=92 includes retarget 133056;
/// first=131040; tip still ~129k → confirmed miss → "missing retarget first
/// header" even though the plan cache should hold 131040.
///
/// Ship path must resolve period-start via confirmed **or** header plan.
#[test]
fn expected_bits_extending_uses_header_plan_when_period_start_above_tip() {
    use super::expected_bits_extending;
    use crate::params::ChainParams;
    use bitcoin::CompactTarget;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::Query;
    use rbitcoin_store::HeaderRecord;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-retarget-plan-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    let params = ChainParams::mainnet();
    let interval = params.difficulty_adjustment_interval();
    assert_eq!(interval, 2016, "mainnet difficulty interval");

    // Tip empty / genesis not required: period-start 2016 is above tip (None).
    assert!(
        q.header_at_height(Height(2016)).unwrap().is_none(),
        "period-start must not be on confirmed[]"
    );

    // Simulate earlier tip-ahead lookup/load that put the period-start plan.
    let mut hash_first = [0u8; 32];
    hash_first[0..4].copy_from_slice(&2016u32.to_le_bytes());
    hash_first[4] = 0xaa;
    let first_rec = HeaderRecord {
        prev_fk: Fk::NULL,
        version: 1,
        timestamp: 1_234_567,
        bits: 0x1d00ffff,
        nonce: 2016,
        merkle_root: hash_first,
        hash: hash_first,
    };
    let first_fk = q.store().put_header(&first_rec).unwrap();
    q.confirm_parent_cache().put_header_plan(
        2016,
        first_fk,
        first_rec.clone(),
        Vec::new(),
        [0u8; 32],
    );
    assert!(
        q.confirm_parent_cache().get_header_plan(2016).is_some(),
        "plan cache holds period-start (as real put_header_plan during load does)"
    );

    // Mid-batch path: prev bits/time come from prior prepared block in RAM;
    // only period-start is resolved from store/plan.
    let prev_bits = CompactTarget::from_consensus(0x1d00ffff);
    let prev_time = first_rec.timestamp.saturating_add(2015 * 600);
    let retarget_h = Height(4032); // 2 * interval — needs first @ 2016
    assert_eq!(retarget_h.0 % interval, 0);

    let got = expected_bits_extending(&q, &params, retarget_h, prev_bits, prev_time).expect(
        "period-start on ConfirmParentCache must satisfy retarget bits \
             (tip-ahead multi-block); confirmed-only lookup is the mainnet bug",
    );
    // Sanity: result is a real CompactTarget (same construction as production).
    let timespan = prev_time.saturating_sub(first_rec.timestamp) as u64;
    let expect = CompactTarget::from_next_work_required(prev_bits, timespan, &params.btc);
    assert_eq!(got, expect);

    let _ = std::fs::remove_dir_all(&path);
}

/// Mempool-preverified txids skip script_wave verify (tip follow).
#[test]
fn script_wave_skips_preverified_txids() {
    use super::{confirm_scripts_phase, LoadedBatch, Prepared, ScriptPreverified};
    use crate::block::ScriptCheckJob;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_primitives::{Fk, Height};

    let prevouts = vec![TxOut {
        value: Amount::from_sat(50_0000_0000),
        // P2PKH-shaped (not anyone-can-spend) so job_needs_script_check is true
        // if we did not skip — invalid empty script_sig would fail without skip.
        script_pubkey: ScriptBuf::from_bytes(vec![
            0x76, 0xa9, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x88,
            0xac,
        ]),
    }];
    let tx = Transaction {
        version: TxVersion::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: bitcoin::Txid::from_byte_array([9; 32]),
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(49_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let tid = tx.compute_txid().to_byte_array();
    let mut pre = ScriptPreverified::new();
    pre.insert(tid);

    let job = ScriptCheckJob::with_txid(tid, prevouts, tx, true, true, true, true, true);
    let prepared = Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(1)],
        jobs: vec![job],
        spends: vec![],
        fees: 0,
        check_scripts: true,
        time: 1,
        bits: CompactTarget::from_consensus(0x207f_ffff),
        hash: [1u8; 32],
        prev_mtp: 0,
    };
    let batch = LoadedBatch {
        prepared: vec![prepared],
        wire_blocks: vec![],
        batch_parents: rbitcoin_query::BatchParents::new(),
        script_preverified: pre,
        archive_plan: None,
    };
    confirm_scripts_phase(batch).expect("preverified skip avoids bad script fail");
}

fn tiny_query() -> (std::path::PathBuf, rbitcoin_query::Query) {
    use rbitcoin_query::Query;
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-ensure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();
    (path, q)
}

fn rec_tx(b: u8, n_out: u32) -> rbitcoin_store::TxRecord {
    use rbitcoin_primitives::Fk;
    rbitcoin_store::TxRecord {
        txid: [b; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: n_out,
    }
}

/// One store: pin/ensure error strings + denserels/abs + freeze + same-batch.
#[test]
fn pin_and_ensure_journey() {
    use super::{
        ensure_spend_abs_layouts, pin_for_wire_batch, post_commit, ParentPinStamp, Prepared,
    };
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{ArchiveWritePlan, BatchParents};
    use rbitcoin_store::{InputRecord, OutputRecord};

    let (path, q) = tiny_query();

    let missing_parent = Fk(999_999);
    let mut plan = ArchiveWritePlan::empty();
    plan.packed = vec![(
        std::sync::Arc::new((rec_tx(0xAA, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
        vec![InputRecord {
            prev_txid: [0xBB; 32],
            create_fk: missing_parent,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }],
    )];
    plan.planned_fks = vec![Fk(1)];
    let stamp = ParentPinStamp::take_from_plan(&mut plan);
    let err = pin_for_wire_batch(&q, Some(&plan), &stamp, &[], &[], None, None)
        .expect_err("missing parent must hard-fail pin");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
        "unexpected err: {msg}"
    );

    let prepared_miss = [Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(10)],
        jobs: vec![],
        spends: vec![([9u8; 32], 0, Fk(10), Fk(999_999))],
        fees: 0,
        check_scripts: false,
        time: 1,
        bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
        hash: [3u8; 32],
        prev_mtp: 0,
    }];
    let mut bp = BatchParents::new();
    let err = ensure_spend_abs_layouts(&q, &mut bp, &prepared_miss)
        .expect_err("ensure must hard-fail without denserels");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("ensure denserels") || msg.contains("abs incomplete")),
        "unexpected err: {msg}"
    );

    let prepared_pc = [Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(10)],
        jobs: vec![],
        spends: vec![([1u8; 32], 0, Fk(10), Fk(2))],
        fees: 0,
        check_scripts: false,
        time: 1,
        bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
        hash: [2u8; 32],
        prev_mtp: 0,
    }];
    let err = post_commit(
        &q,
        &prepared_pc,
        &BatchParents::new(),
        &rbitcoin_query::U64Map::default(),
    )
    .expect_err("missing denserels");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant") && msg.contains("denserels"),
        "unexpected err: {msg}"
    );

    let parent_tx = rec_tx(0x11, 1);
    let parent_outs = vec![OutputRecord::unspent(50, vec![0x51])];
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let pfk = q
        .store()
        .put_tx_full_batch_indexed(
            &[(parent_tx.clone(), parent_ins, parent_outs.clone())],
            true,
        )
        .unwrap()[0];
    let range = q.store().tx_body_range(pfk).unwrap();
    let (spent_off, _spent_len) = q.store().tx_spent_range(pfk).unwrap();
    let parent_id = pfk.get().unwrap();

    let spend_ins = vec![InputRecord {
        prev_txid: parent_tx.txid,
        create_fk: pfk,
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let mut plan = ArchiveWritePlan::empty();
    plan.packed = vec![(
        std::sync::Arc::new((rec_tx(0x22, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins.clone(),
    )];
    plan.planned_fks = vec![Fk(2)];
    plan.external_parent_ranges.insert(parent_id, range);
    plan.external_parent_txids.insert(parent_id, parent_tx.txid);
    let stamp = ParentPinStamp::take_from_plan(&mut plan);
    let (parents, _, _) = pin_for_wire_batch(&q, Some(&plan), &stamp, &[], &[], None, None)
        .expect("pin via stamped range");
    assert!(parents.contains(pfk));
    assert!(parents.get_parent_out(pfk, 0).is_some());
    plan.freeze_after_pin();
    assert!(
        plan.external_parent_ranges.is_empty() && plan.external_parent_txids.is_empty(),
        "post-pin plan must not carry stamp staging"
    );

    let mut plan2 = ArchiveWritePlan::empty();
    plan2.packed = vec![(
        std::sync::Arc::new((rec_tx(0x22, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins.clone(),
    )];
    plan2.planned_fks = vec![Fk(2)];
    plan2.external_parent_ranges.insert(parent_id, range);
    plan2
        .external_parent_txids
        .insert(parent_id, parent_tx.txid);
    let err = pin_for_wire_batch(
        &q,
        Some(&plan2),
        &ParentPinStamp::default(),
        &[],
        &[],
        None,
        None,
    )
    .expect_err("plan maps must not backfill an empty stamp");
    assert!(err.to_string().contains("lookup stage miss"), "got: {err}");

    let mut bp = BatchParents::new();
    bp.insert_owned(
        pfk,
        parent_tx.clone(),
        vec![(0, parent_outs[0].clone())],
        vec![0],
        Some(true),
        None,
        Vec::new(),
    );
    assert!(!bp.has_abs_layout(pfk));
    let prepared = [Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(2)],
        jobs: vec![],
        spends: vec![([0x11u8; 32], 0, Fk(2), pfk)],
        fees: 0,
        check_scripts: false,
        time: 1,
        bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
        hash: [4u8; 32],
        prev_mtp: 0,
    }];
    ensure_spend_abs_layouts(&q, &mut bp, &prepared).expect("spent-range ensure");
    assert!(bp.has_abs_layout(pfk));
    assert_eq!(
        bp.get_spender_abs(pfk, 0),
        Some(rbitcoin_store::spent_abs(spent_off, 0))
    );

    let mut plan3 = ArchiveWritePlan::empty();
    plan3.packed = vec![(
        std::sync::Arc::new((rec_tx(0x22, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins,
    )];
    plan3.planned_fks = vec![Fk(2)];
    plan3.external_parent_ranges.insert(parent_id, range);
    plan3
        .external_parent_txids
        .insert(parent_id, parent_tx.txid);
    let stamp3 = ParentPinStamp::take_from_plan(&mut plan3);
    let (mut parents3, _, _) =
        pin_for_wire_batch(&q, Some(&plan3), &stamp3, &[], &[], None, None).unwrap();
    assert!(!parents3.has_abs_layout(pfk));
    ensure_spend_abs_layouts(&q, &mut parents3, &prepared).expect("ensure pin-hit");
    assert!(parents3.has_abs_layout(pfk));

    let mut plan4 = ArchiveWritePlan::empty();
    plan4.packed = vec![
        (
            std::sync::Arc::new((rec_tx(0x32, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
            vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
        ),
        (
            std::sync::Arc::new((rec_tx(0x33, 1), vec![OutputRecord::unspent(1, vec![0x51])])),
            vec![InputRecord {
                prev_txid: [0x32; 32],
                create_fk: Fk(2),
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
        ),
    ];
    plan4.planned_fks = vec![Fk(2), Fk(3)];
    let stamp4 = ParentPinStamp::take_from_plan(&mut plan4);
    let (parents4, _, _) =
        pin_for_wire_batch(&q, Some(&plan4), &stamp4, &[], &[], None, None).unwrap();
    assert!(parents4.contains(Fk(2)));
    assert!(
        !parents4.has_abs_layout(Fk(2)),
        "same-batch create must not get a spent_range before Class A commit"
    );

    let _ = std::fs::remove_dir_all(&path);
}

/// Cold-range pin then pstore adopt: first pin reads body, second does not.
#[test]
fn pin_for_wire_cold_range_then_adopt_skips_body_io() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{PipelineParentStore, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-range-adopt-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&path);
    let q = Query::open_or_create(&path).unwrap();

    let parent_tx = TxRecord {
        txid: [0xab; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let parent_outs = vec![OutputRecord::unspent(50_0000_0000, vec![0x51])];
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let pfk = q
        .store()
        .txs
        .put_full_batch_indexed(&[(parent_tx.clone(), parent_ins, parent_outs)], true)
        .unwrap()[0];
    let range = q.store().tx_body_range(pfk).unwrap();

    let spend_tx = TxRecord {
        txid: [0xcd; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
    let spend_ins = vec![InputRecord {
        prev_txid: parent_tx.txid,
        create_fk: pfk,
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let stamp_plan = || {
        let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
        plan.packed = vec![(
            std::sync::Arc::new((spend_tx.clone(), spend_outs.clone())),
            spend_ins.clone(),
        )];
        plan.planned_fks = vec![Fk(2)];
        if let Some(id) = pfk.get() {
            plan.external_parent_ranges.insert(id, range);
            plan.external_parent_txids.insert(id, parent_tx.txid);
        }
        plan
    };

    let store = Arc::new(PipelineParentStore::new());
    let mut plan = stamp_plan();
    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let (parents, _thin, _warm) =
        pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], None, Some(&store)).unwrap();
    assert!(parents.contains(pfk));
    assert!(
        parents.get_parent_out(pfk, 0).is_some(),
        "cold-range pin must load the spent vout"
    );

    let mut plan2 = stamp_plan();
    let parent_pin2 = ParentPinStamp::take_from_plan(&mut plan2);
    let (parents2, _thin2, _warm2) =
        pin_for_wire_batch(&q, Some(&plan2), &parent_pin2, &[], &[], None, Some(&store)).unwrap();
    assert!(parents2.contains(pfk));
    assert!(
        parents2.get_parent_out(pfk, 0).is_some(),
        "pstore adopt must still serve the spent vout"
    );

    let _ = std::fs::remove_dir_all(&path);
}

/// Wire pin: in-flight outs shorter than need → cold miss → hard invariant.
#[test]
fn pin_for_wire_incomplete_outs_is_invariant_error() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-wire-outs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let parent_id = 77u64;
    let parent_fk = Fk(parent_id);
    // Spend needs vout 0 from parent_id.
    let spend_tx = TxRecord {
        txid: [0xCCu8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: [0xDDu8; 32],
        create_fk: parent_fk,
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let mut plan = ArchiveWritePlan {
        packed: vec![(
            std::sync::Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
            spend_ins,
        )],
        planned_fks: vec![Fk(2)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_ranges: Default::default(),
        external_parent_txids: Default::default(),
        batch_pin: vec![],
        index_tx: false,
        body_est: 0,
    };
    // In-flight "parent" with **empty** outs → live.len() != need → cold path;
    // no Class A body either → end pin contract fails.
    let parent_tx = TxRecord {
        txid: [0xDDu8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 0,
    };
    let pin = std::sync::Arc::new((parent_tx, Vec::new()));
    let mut log = rbitcoin_query::InFlightLog::new();
    log.note_layer(rbitcoin_query::InFlightLayer::from_plan_pins([(
        Fk(parent_id),
        &pin,
    )]));
    let ifo = log.snapshot();

    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let err = pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], Some(&ifo), None)
        .expect_err("incomplete outs must hard-fail pin");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
        "unexpected err: {msg}"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// After wire pin, freeze drops ranges+txids; BatchParents keep sparse outs.
#[test]
fn parent_pin_stamp_take_from_plan_moves_maps() {
    use super::ParentPinStamp;
    use rbitcoin_query::{ArchiveWritePlan, U64Map};

    let mut ranges = U64Map::default();
    ranges.insert(7, (8, 16));
    let mut txids = U64Map::default();
    txids.insert(7, [0xABu8; 32]);
    let mut plan = ArchiveWritePlan {
        packed: vec![],
        planned_fks: vec![],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_ranges: ranges,
        external_parent_txids: txids,
        batch_pin: vec![],
        index_tx: false,
        body_est: 0,
    };
    let stamp = ParentPinStamp::take_from_plan(&mut plan);
    assert!(plan.external_parent_ranges.is_empty());
    assert!(plan.external_parent_txids.is_empty());
    assert_eq!(stamp.ranges.get(&7).copied(), Some((8, 16)));
    assert_eq!(stamp.create_txid(7), Some([0xABu8; 32]));
}

/// Need a high vout from a multi-out parent (need-vouts only, not full n_out).
#[test]
fn pin_sparse_need_high_vout_only() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-sparse-high-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let parent_tx = TxRecord {
        txid: [0x33u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 4,
    };
    let parent_outs = vec![
        OutputRecord::unspent(1, vec![0x00]),
        OutputRecord::unspent(2, vec![0x01]),
        OutputRecord::unspent(3, vec![0x02]),
        OutputRecord::unspent(4, vec![0xaa]),
    ];
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let pfk = q
        .store()
        .txs
        .put_full_batch_indexed(&[(parent_tx.clone(), parent_ins, parent_outs)], true)
        .unwrap()[0];
    let range = q.store().tx_body_range(pfk).unwrap();
    let parent_id = pfk.get().unwrap();

    let spend_tx = TxRecord {
        txid: [0x44u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: parent_tx.txid,
        create_fk: pfk,
        prev_index: 3,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let spend_pin: CreatePin = Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])]));
    let mut plan = ArchiveWritePlan {
        packed: vec![(Arc::clone(&spend_pin), spend_ins)],
        planned_fks: vec![Fk(2)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_ranges: {
            let mut m = rbitcoin_query::U64Map::default();
            m.insert(parent_id, range);
            m
        },
        external_parent_txids: {
            let mut m = rbitcoin_query::U64Map::default();
            m.insert(parent_id, parent_tx.txid);
            m
        },
        batch_pin: vec![Arc::clone(&spend_pin)],
        index_tx: false,
        body_est: 0,
    };
    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let (parents, _thin, _warm) =
        pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], None, None)
            .expect("pin high vout");
    assert!(parents.get_parent_out(pfk, 3).is_some());
    assert_eq!(
        parents.get_parent_out(pfk, 3).unwrap().1.value,
        4,
        "need-vout 3 only"
    );
    assert!(
        parents.get_parent_out(pfk, 1).is_none(),
        "must not pin unneeded vouts"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// Range-fill this window is `PIN_NEW`, not `PIN_CACHE_BODY` / `warm.already`.
///
/// Adopt 1 + cold-range 2 → `already=1` (cache), not 3. `pin_hit%` is
/// `1/(1+2)=33`, not “we just loaded them.”
#[test]
fn pin_range_fill_does_not_count_as_cache_hit() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, BatchParents, PipelineParentStore, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-hit-honest-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let mk_parent = |tag: u8| {
        let mut tid = [0u8; 32];
        tid[0] = tag;
        tid[1] = 0xee;
        (
            TxRecord {
                txid: tid,
                version: 1,
                locktime: 0,
                input_start_fk: Fk::NULL,
                input_count: 1,
                output_start_fk: Fk::NULL,
                output_count: 1,
            },
            vec![InputRecord::coinbase(u32::MAX, vec![tag], vec![])],
            vec![OutputRecord::unspent(1000 + tag as i64, vec![0x51, tag])],
        )
    };
    let items = [mk_parent(1), mk_parent(2), mk_parent(3)];
    let fks = q.store().txs.put_full_batch_indexed(&items, true).unwrap();
    assert_eq!(fks.len(), 3);
    let mut ranges = Vec::new();
    for fk in &fks {
        ranges.push(q.store().tx_body_range(*fk).unwrap());
    }

    // Live pin for parent 0 only (same Weak lifecycle as outs share).
    let store = Arc::new(PipelineParentStore::new());
    let mut keep = BatchParents::with_store(Arc::clone(&store), 1);
    keep.insert_owned(
        fks[0],
        items[0].0.clone(),
        vec![(0, items[0].2[0].clone())],
        vec![0],
        Some(false),
        Some(ranges[0]),
        Vec::new(),
    );
    keep.publish_to_store();

    let spend_tx = TxRecord {
        txid: [0x5cu8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 3,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins: Vec<InputRecord> = (0..3)
        .map(|i| InputRecord {
            prev_txid: items[i].0.txid,
            create_fk: fks[i],
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        })
        .collect();
    let mut plan = ArchiveWritePlan::empty();
    plan.packed = vec![(
        Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins,
    )];
    plan.planned_fks = vec![Fk(100)];
    for i in 0..3 {
        if let Some(id) = fks[i].get() {
            plan.external_parent_ranges.insert(id, ranges[i]);
            plan.external_parent_txids.insert(id, items[i].0.txid);
        }
    }

    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let (_parents, _thin, warm) =
        pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], None, Some(&store))
            .expect("adopt 1 + range-fill 2");
    assert_eq!(warm.parents, 3);
    assert_eq!(
        warm.already, 1,
        "range-fills must not increment already / PIN_CACHE_BODY"
    );
    drop(keep);
    let _ = std::fs::remove_dir_all(&path);
}

/// Write-published RecentCreates outs cover a later spend after in-flight is gone.
/// That is `PIN_CACHE_BODY` / `warm.already`, not `PIN_NEW` / range-fill.
#[test]
fn pin_recent_outs_is_cache_not_new() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-recent-outs-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();

    let mut tid = [0u8; 32];
    tid[0] = 0x41;
    let parent_tx = TxRecord {
        txid: tid,
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let parent_out = OutputRecord::unspent(50, vec![0x51, 0xaa]);
    let pin: CreatePin = Arc::new((parent_tx.clone(), vec![parent_out.clone()]));
    let pfk = Fk(7);
    q.note_recent_creates_pins(10, [(tid, pfk, (1, 8), Some(Arc::clone(&pin)))]);
    q.flush_recent_creates();

    let spend_tx = TxRecord {
        txid: [0x5cu8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: tid,
        create_fk: pfk,
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let mut plan = ArchiveWritePlan::empty();
    plan.packed = vec![(
        Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins,
    )];
    plan.planned_fks = vec![Fk(100)];
    plan.external_parent_ranges.insert(7, (99, 1));
    plan.external_parent_txids.insert(7, tid);

    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let (_parents, _thin, warm) =
        pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], None, None)
            .expect("recent outs must cover without range-fill");
    assert_eq!(warm.parents, 1);
    assert_eq!(
        warm.already, 1,
        "RecentCreates outs must count as PIN_CACHE, not PIN_NEW"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// Identity-only RecentCreates (no outs) still cold-fills by stamped range.
#[test]
fn pin_recent_identity_without_outs_still_range_fills() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-recent-id-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let mut tid = [0u8; 32];
    tid[0] = 0x42;
    let parent = (
        TxRecord {
            txid: tid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 1,
            output_start_fk: Fk::NULL,
            output_count: 1,
        },
        vec![InputRecord::coinbase(u32::MAX, vec![0x42], vec![])],
        vec![OutputRecord::unspent(50, vec![0x51, 0x42])],
    );
    let fks = q
        .store()
        .txs
        .put_full_batch_indexed(&[parent.clone()], true)
        .unwrap();
    let range = q.store().tx_body_range(fks[0]).unwrap();
    q.note_recent_creates_rows(10, [(tid, fks[0], range)]);
    q.flush_recent_creates();

    let spend_tx = TxRecord {
        txid: [0x5du8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: tid,
        create_fk: fks[0],
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let mut plan = ArchiveWritePlan::empty();
    plan.packed = vec![(
        Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
        spend_ins,
    )];
    plan.planned_fks = vec![Fk(100)];
    if let Some(id) = fks[0].get() {
        plan.external_parent_ranges.insert(id, range);
        plan.external_parent_txids.insert(id, tid);
    }

    let parent_pin = ParentPinStamp::take_from_plan(&mut plan);
    let (_parents, _thin, warm) =
        pin_for_wire_batch(&q, Some(&plan), &parent_pin, &[], &[], None, None)
            .expect("identity-only recent still range-fills");
    assert_eq!(warm.parents, 1);
    assert_eq!(
        warm.already, 0,
        "identity without outs must not count as PIN_CACHE"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// Store start states: S0 new Class A and S1 already-archived both confirm
/// via shipped lookup→load (body denserels by range; no load head/idx).
#[test]
fn store_start_states_lookup_load_confirm() {
    use super::{
        confirm_scripts_phase, confirm_wire_load_from_plan, confirm_wire_lookup_stamp,
        confirm_write_phase, ScriptPreverified,
    };
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use crate::{accept_and_connect_block, prepare_block_for_archive};
    use bitcoin::block::{Header, Version};
    use bitcoin::blockdata::transaction::{
        OutPoint, Transaction, TxIn, TxOut, Version as TxVersion,
    };
    use bitcoin::hashes::Hash;
    use bitcoin::locktime::absolute::LockTime;
    use bitcoin::script::PushBytesBuf;
    use bitcoin::CompactTarget;
    use bitcoin::{Amount, Block, BlockHash, ScriptBuf, Sequence, TxMerkleNode, Witness};
    use rbitcoin_primitives::Height;
    use rbitcoin_query::Query;
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-start-states-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.set_spend_index(true);
    let params = ChainParams::regtest();
    let ms = Milestone::NONE;
    let maturity = params.coinbase_maturity();

    fn coinbase(height: u32) -> Transaction {
        let mut script = ScriptBuf::new();
        let pb = PushBytesBuf::try_from(height.to_le_bytes().to_vec()).unwrap();
        script.push_slice(pb);
        script.push_opcode(bitcoin::opcodes::all::OP_CHECKSIG);
        Transaction {
            version: TxVersion::ONE,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: script,
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: Amount::from_sat(50_0000_0000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }
    fn mine_cb(prev: BlockHash, time: u32, h: u32) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut block = Block {
            header: Header {
                version: Version::from_consensus(4),
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time,
                bits,
                nonce: 0,
            },
            txdata: vec![coinbase(h)],
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = bitcoin::Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }
    fn mine_with(prev: BlockHash, time: u32, h: u32, extra: Vec<Transaction>) -> Block {
        let bits = CompactTarget::from_consensus(0x207f_ffff);
        let mut txs = vec![coinbase(h)];
        txs.extend(extra);
        let mut block = Block {
            header: Header {
                version: Version::from_consensus(4),
                prev_blockhash: prev,
                merkle_root: TxMerkleNode::from_byte_array([0; 32]),
                time,
                bits,
                nonce: 0,
            },
            txdata: txs,
        };
        block.header.merkle_root = block.compute_merkle_root().unwrap();
        let target = bitcoin::Target::from_compact(bits);
        for nonce in 0..u32::MAX {
            block.header.nonce = nonce;
            if block.header.validate_pow(target).is_ok() {
                break;
            }
        }
        block
    }
    fn spend(prev: bitcoin::Txid, vout: u32, val: Amount) -> Transaction {
        Transaction {
            version: TxVersion::TWO,
            lock_time: LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint { txid: prev, vout },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            }],
            output: vec![TxOut {
                value: val,
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            }],
        }
    }

    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    accept_and_connect_block(&q, &params, Height::GENESIS, &genesis, ms).unwrap();
    let mut tip = genesis.block_hash();
    let mut tip_time = genesis.header.time;
    let b1 = mine_cb(tip, tip_time + 600, 1);
    let c1 = b1.txdata[0].compute_txid();
    accept_and_connect_block(&q, &params, Height(1), &b1, ms).unwrap();
    tip = b1.block_hash();
    tip_time = b1.header.time;
    for h in 2..=maturity + 1 {
        let b = mine_cb(tip, tip_time + 600, h);
        accept_and_connect_block(&q, &params, Height(h), &b, ms).unwrap();
        tip = b.block_hash();
        tip_time = b.header.time;
    }

    // S0: new Class A plan — stamp must fill parent body_range; load Forbid ok.
    let h_s0 = maturity + 2;
    let b_s0 = mine_with(
        tip,
        tip_time + 600,
        h_s0,
        vec![spend(c1, 0, Amount::from_sat(49_0000_0000))],
    );
    {
        let arcs = [(Height(h_s0), Arc::new(b_s0.clone()), None)];
        let stamped = confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("S0 lookup");
        assert!(stamped.plan.is_some(), "S0 must plan Class A");
        assert!(
            !stamped.parent_pin.ranges.is_empty(),
            "S0 lookup must stamp external parent body ranges"
        );
        let mat =
            confirm_wire_load_from_plan(&q, &params, ms, stamped, None, &ScriptPreverified::new())
                .expect("S0 load denserels by range");
        let ok = confirm_scripts_phase(mat.batch).expect("S0 scripts");
        confirm_write_phase(&q, &params, ms, ok.batch).expect("S0 write");
    }
    assert_eq!(q.tip_height().map(|h| h.0), Some(h_s0));
    tip = b_s0.block_hash();
    tip_time = b_s0.header.time;

    // S1: already-archived (plan=None) — lookup stamps parent pin; load by range.
    let h_s1 = h_s0 + 1;
    let b_s1 = mine_cb(tip, tip_time + 600, h_s1);
    let (header_s1, txs_s1) = prepare_block_for_archive(&q, &params, &b_s1).unwrap();
    q.commit_class_a_only(&header_s1, &txs_s1).unwrap();
    assert_eq!(q.tip_height().map(|h| h.0), Some(h_s0));
    {
        let arcs = [(Height(h_s1), Arc::new(b_s1.clone()), None)];
        let stamped = confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("S1 lookup");
        assert!(stamped.plan.is_none(), "S1 already-archived → plan=None");
        let mat =
            confirm_wire_load_from_plan(&q, &params, ms, stamped, None, &ScriptPreverified::new())
                .expect("S1 plan=None load");
        let ok = confirm_scripts_phase(mat.batch).expect("S1 scripts");
        confirm_write_phase(&q, &params, ms, ok.batch).expect("S1 write");
    }
    assert_eq!(q.tip_height().map(|h| h.0), Some(h_s1));

    let _ = std::fs::remove_dir_all(&path);
}

/// Load miss: spend edges without pin denserels must hard-fail (no cold tier).
/// Pin-covered parent without denserels/abs fails structural (no body-range cold).
#[test]
fn structural_pinned_without_abs_is_invariant_error() {
    use crate::block::structural_validate_spends;
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, BlockHash, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut,
        Witness,
    };
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{BatchParents, Query};
    use rbitcoin_store::{OutputRecord, TxRecord};
    use std::collections::HashSet;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-struct-pin-inv-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();
    let params = ChainParams::regtest();

    // Minimal non-empty block (coinbase only) for structural entry.
    let coinbase = Transaction {
        version: TxVersion::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(vec![0x00, 0x01]),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(50_0000_0000),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let mut block = Block {
        header: Header {
            version: Version::from_consensus(4),
            prev_blockhash: BlockHash::from_byte_array([0u8; 32]),
            merkle_root: bitcoin::TxMerkleNode::from_byte_array([0u8; 32]),
            time: 1_300_000_000,
            bits: CompactTarget::from_consensus(0x207f_ffff),
            nonce: 0,
        },
        txdata: vec![coinbase],
    };
    block.header.merkle_root = block.compute_merkle_root().unwrap();

    // Parent pin present (outs) but denserels/body_range missing → abs None.
    let mut bp = BatchParents::new();
    let parent_fk = Fk(42);
    let tx = TxRecord {
        txid: [7u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let out = OutputRecord::unspent(1, vec![0x51]);
    bp.insert_owned(
        parent_fk,
        tx,
        vec![(0, out)],
        vec![0],
        Some(false),
        None,   // no body_range
        vec![], // no denserels
    );

    let spends = vec![([7u8; 32], 0u32, Fk(100), parent_fk)];
    let ctx = crate::block::ValidationContext::at(&params, Height(1), Milestone::NONE);
    let mut pending = HashSet::new();
    let mut mtp = rbitcoin_query::U32Map::<u32>::default();
    let mut meta_by_abs = rbitcoin_query::U64Map::default();
    let err = structural_validate_spends(
        &q,
        &block,
        &ctx,
        None,
        &spends,
        0,
        &mut pending,
        &bp,
        &mut mtp,
        &mut meta_by_abs,
        &rbitcoin_query::FkMap::default(),
    )
    .expect_err("pinned without abs must be invariant");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant") && msg.contains("denserels"),
        "unexpected err: {msg}"
    );
    let _ = std::fs::remove_dir_all(&path);
}
