//! Confirm_run unit tests (peeled from confirm_run.rs).

use super::write_height_needed;

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

/// denserels ensure with no plan is a pure no-op warm path.
#[test]
fn ensure_external_parent_denserels_none_plan_is_noop() {
    use super::ensure_external_parent_denserels_from_plan;
    use rbitcoin_query::Query;
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-denserels-none-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    let st = ensure_external_parent_denserels_from_plan(&q, None, None).unwrap();
    assert_eq!(st.parents, 0);
    // Empty plan mut ref also no-ops past collect.
    let mut empty = rbitcoin_query::ArchiveWritePlan::empty();
    let st2 = ensure_external_parent_denserels_from_plan(&q, Some(&mut empty), None).unwrap();
    assert_eq!(st2.parents, 0);
    let _ = std::fs::remove_dir_all(&path);
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

/// `confirm_scripts_phase_async` must not occupy a steal worker (`rbtc-scripts-*`).
///
/// Thread name is recorded on the handle (not process-global
/// [`scripts_feed_test_sync`]) so a parallel `reset()` / `on_phase_enter`
/// cannot steal or overwrite it under `cargo llvm-cov`.
#[test]
fn scripts_phase_does_not_run_on_steal_worker() {
    use super::confirm_scripts_phase_async;
    let (ok, name) = confirm_scripts_phase_async(empty_loaded_batch())
        .join_with_phase_thread()
        .expect("empty phase");
    assert!(ok.batch.is_empty());
    assert!(
        name.starts_with("rbtc-script-coord-"),
        "scripts phase must run on a coordinator, got {name:?}"
    );
    assert!(
        !name.starts_with("rbtc-scripts-"),
        "scripts phase ran on steal worker {name:?}"
    );
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

/// **Production claim timing under depth-1:** batch B is submitted to a
/// coordinator while A’s wave is still open (not only after A’s join returns).
///
/// Drives [`scripts_stage_from_load_channel`] (same `try_recv` +
/// [`join_scripts_polling`] pattern as the IBD scripts OS thread) on a
/// `sync_channel(1)`. First wave holds in [`confirm_scripts_phase`] until
/// a second async submit is observed — deadlocks if feed-ahead only
/// try_recv once before a blocking join.
#[test]
fn scripts_stage_depth1_submits_second_before_first_finishes() {
    use super::{
        scripts_feed_test_sync, scripts_stage_from_load_channel, ConfirmScriptOutcome,
        ScriptsBatchMeta,
    };
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::{Duration, Instant};

    scripts_feed_test_sync::reset();
    scripts_feed_test_sync::set_hold_first_until_second_submit(true);

    // Depth 1 — same default load→scripts capacity class.
    let (mat_tx, mat_rx) = mpsc::sync_channel::<(super::LoadedBatch, u64)>(1);
    let outcomes: Arc<Mutex<Vec<ConfirmScriptOutcome>>> = Arc::new(Mutex::new(Vec::new()));
    let outcomes_w = Arc::clone(&outcomes);

    let stage = thread::spawn(move || {
        scripts_stage_from_load_channel(
            &mat_rx,
            |ok, _meta: ScriptsBatchMeta| {
                outcomes_w.lock().unwrap().push(ok);
                true
            },
            |_e, _meta| false,
            || false,
        );
    });

    // Enqueue A; stage claims it (channel free). Hold keeps A's phase open.
    mat_tx.send((empty_loaded_batch(), 0)).expect("send A");
    let deadline = Instant::now() + Duration::from_secs(3);
    while scripts_feed_test_sync::submit_count() < 1 {
        assert!(
            Instant::now() < deadline,
            "A never submitted to coordinator"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // Enqueue B while A is held mid-wave; feed-ahead must try_recv+submit B.
    mat_tx
        .send((empty_loaded_batch(), 0))
        .expect("send B while A verifying");
    while scripts_feed_test_sync::submit_count() < 2 {
        assert!(
            Instant::now() < deadline,
            "B not submitted before A finished (feed-ahead dead under depth-1)"
        );
        thread::sleep(Duration::from_millis(1));
    }
    // A can finish (hold released by submit_count>=2); both outcomes ordered.
    drop(mat_tx);
    stage.join().expect("stage thread");
    let outs = outcomes.lock().unwrap();
    assert_eq!(outs.len(), 2, "both batches script-ok");
    assert!(outs[0].batch.is_empty());
    assert!(outs[1].batch.is_empty());
    scripts_feed_test_sync::set_hold_first_until_second_submit(false);
    scripts_feed_test_sync::reset();
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
            version: Version::ONE,
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
            version: Version::ONE,
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
    use crate::confirm_phase_stats;
    use bitcoin::absolute::LockTime;
    use bitcoin::hashes::Hash;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};
    use rbitcoin_primitives::{Fk, Height};
    use std::sync::atomic::Ordering;

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
    let before = confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.load(Ordering::Relaxed);
    confirm_scripts_phase(batch).expect("preverified skip avoids bad script fail");
    let after = confirm_phase_stats::SCRIPT_SKIP_MEMPOOL.load(Ordering::Relaxed);
    assert!(after > before, "skip counter should bump");
}

/// Lookup-stage denserels ensure + Forbid pin: cold path must not re-run on load.
/// External parents land in plan-local map only.
#[test]
fn plan_ensure_denserels_then_forbid_skips_cold_io() {
    use super::{ensure_external_parent_denserels_from_plan, pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::Query;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-plan-ensure-{}-{}",
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
    // Parent on disk only (ancient / cold external parent).

    // Plan with stamped parent create_fk (lookup stage already did batch head).
    let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
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
    plan.packed = vec![(
        std::sync::Arc::new((spend_tx, spend_outs)),
        vec![InputRecord {
            prev_txid: parent_tx.txid,
            create_fk: pfk,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }],
    )];
    plan.planned_fks = vec![Fk(2)];

    rbitcoin_query::reset_body_ok_reads();
    let st = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
    assert!(
        st.cold >= 1,
        "parent missing denserels must cold-load: {st:?}"
    );
    assert!(
        plan.external_parent_outs
            .get(&pfk.get().unwrap())
            .is_some_and(|p| !p.1.is_empty()),
        "ensure must put sparse denserels in plan-local external_parent_outs"
    );
    // Sparse only — no full output_count expand (parent has 1 out here; multi-out
    // sparse regression covers high output_count without n_out alloc).
    if let Some(p) = plan.external_parent_outs.get(&pfk.get().unwrap()) {
        assert_eq!(p.1.len(), 1, "sparse live must be need-vouts only");
        assert!(
            p.1.iter().all(|(v, _)| *v == 0),
            "sparse live keyed by vout, not dense index"
        );
    }
    let reads_after = rbitcoin_query::body_ok_reads();

    // Second ensure: plan-local already present → no more body IO.
    let st2 = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
    assert!(st2.already >= 1 && st2.cold == 0, "st2={st2:?}");
    assert_eq!(
        rbitcoin_query::body_ok_reads(),
        reads_after,
        "already-warm denserels must not re-read body"
    );

    // Pin Forbid hits plan-local (no extra cold).
    let (parents, _thin, _warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .unwrap();
    assert!(parents.contains(pfk));
    assert_eq!(
        rbitcoin_query::body_ok_reads(),
        reads_after,
        "pin after plan ensure must not cold denserels again"
    );

    let _ = std::fs::remove_dir_all(&path);
}

/// Wire pin: spend parent not loadable → hard invariant (no silent skip).
#[test]
fn pin_for_wire_missing_parent_is_invariant_error() {
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
        "rbitcoin-pin-wire-inv-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    // Plan create spends external create_fk that has no Class A body / residency.
    let missing_parent = Fk(999_999);
    let spend_tx = TxRecord {
        txid: [0xAAu8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: [0xBBu8; 32],
        create_fk: missing_parent,
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
    let plan = ArchiveWritePlan {
        packed: vec![(std::sync::Arc::new((spend_tx, spend_outs)), spend_ins)],
        planned_fks: vec![Fk(1)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_outs: Default::default(),
        external_parent_ranges: Default::default(),
        external_parent_txids: Default::default(),
        batch_pin: vec![],
        index_tx: false,
        body_est: 0,
    };

    let err = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .expect_err("missing parent must hard-fail pin");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
        "unexpected err: {msg}"
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
    let plan = ArchiveWritePlan {
        packed: vec![(
            std::sync::Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
            spend_ins,
        )],
        planned_fks: vec![Fk(2)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_outs: Default::default(),
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

    let err = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        Some(&ifo),
        None,
    )
    .expect_err("incomplete outs must hard-fail pin");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("wire pin") || msg.contains("lookup stage miss")),
        "unexpected err: {msg}"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// After wire pin, external sparse outs are cleared; sparse BatchParents remain.
/// Pin uses Arc::clone of SparseExternalPin (no deep outs clone).
#[test]
fn pin_takes_external_create_pin_arc_then_clear_for_write_queue() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query, SparseExternalPin};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::{Arc, Once};

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-pin-external-clear-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let parent_id = 1u64;
    let parent_tx = TxRecord {
        txid: [0x11u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let parent_out = OutputRecord::unspent(50_0000_0000, vec![0x51]);
    let external: SparseExternalPin = Arc::new((parent_tx.clone(), vec![(0, parent_out)]));

    let spend_tx = TxRecord {
        txid: [0x22u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_ins = vec![InputRecord {
        prev_txid: parent_tx.txid,
        create_fk: Fk(parent_id),
        prev_index: 0,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
    let spend_pin: CreatePin = Arc::new((spend_tx, spend_outs));

    let mut plan = ArchiveWritePlan {
        packed: vec![(Arc::clone(&spend_pin), spend_ins)],
        planned_fks: vec![Fk(2)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_outs: {
            let mut m = rbitcoin_query::U64Map::default();
            m.insert(parent_id, Arc::clone(&external));
            m
        },
        external_parent_ranges: Default::default(),
        external_parent_txids: Default::default(),
        batch_pin: vec![Arc::clone(&spend_pin)],
        index_tx: false,
        body_est: 0,
    };

    // Map holds the same Arc as our local handle (not a deep clone of outs).
    assert!(Arc::ptr_eq(
        plan.external_parent_outs.get(&parent_id).unwrap(),
        &external
    ));
    let (parents, _thin, _warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .expect("pin external via SparseExternalPin Arc (body denserels by range only)");
    assert!(parents.contains(Fk(parent_id)));
    assert!(
        parents.get_parent_out(Fk(parent_id), 0).is_some(),
        "sparse need-vout must be in BatchParents"
    );
    // Plan map still the shared Arc until load clears it.
    assert!(Arc::ptr_eq(
        plan.external_parent_outs.get(&parent_id).unwrap(),
        &external
    ));

    // Production load freezes plan after pin so write queue is lean.
    plan.freeze_after_pin();
    assert!(
        plan.external_parent_outs.is_empty(),
        "post-pin plan must not carry external sparse outs to scripts/write"
    );
    // Sparse pin still holds the need-vout independently of the plan map.
    assert!(parents.get_parent_out(Fk(parent_id), 0).is_some());
    let _ = std::fs::remove_dir_all(&path);
}

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
        external_parent_outs: Default::default(),
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

/// Need a high vout from a multi-out sparse pin (binary search, not linear find).
#[test]
fn pin_sparse_need_high_vout_only() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::{ArchiveWritePlan, CreatePin, Query, SparseExternalPin};
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

    let parent_id = 1u64;
    let parent_tx = TxRecord {
        txid: [0x33u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 4,
    };
    let external: SparseExternalPin = Arc::new((
        parent_tx.clone(),
        vec![
            (0, OutputRecord::unspent(1, vec![0x00])),
            (1, OutputRecord::unspent(2, vec![0x01])),
            (2, OutputRecord::unspent(3, vec![0x02])),
            (3, OutputRecord::unspent(4, vec![0xaa])),
        ],
    ));
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
        create_fk: Fk(parent_id),
        prev_index: 3,
        sequence: u32::MAX,
        script_sig: vec![],
        witness: vec![],
    }];
    let spend_pin: CreatePin = Arc::new((spend_tx, vec![OutputRecord::unspent(1, vec![0x51])]));
    let plan = ArchiveWritePlan {
        packed: vec![(Arc::clone(&spend_pin), spend_ins)],
        planned_fks: vec![Fk(2)],
        per_header_ranges: vec![],
        spends: vec![],
        batch_creates: vec![],
        external_parent_outs: {
            let mut m = rbitcoin_query::U64Map::default();
            m.insert(parent_id, external);
            m
        },
        external_parent_ranges: Default::default(),
        external_parent_txids: Default::default(),
        batch_pin: vec![Arc::clone(&spend_pin)],
        index_tx: false,
        body_est: 0,
    };
    let (parents, _thin, _warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .expect("pin high vout");
    assert!(parents.get_parent_out(Fk(parent_id), 3).is_some());
    assert!(
        parents.get_parent_out(Fk(parent_id), 1).is_none(),
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

    let (_parents, _thin, warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        Some(&store),
    )
    .expect("adopt 1 + range-fill 2");
    assert_eq!(warm.parents, 3);
    assert_eq!(
        warm.already, 1,
        "range-fills must not increment already / PIN_CACHE_BODY"
    );
    drop(keep);
    let _ = std::fs::remove_dir_all(&path);
}

/// Multi-out parent: ensure/pin keep only spent need-vouts (no n_out expand).
#[test]
fn ensure_external_sparse_need_not_full_output_count() {
    use super::{ensure_external_parent_denserels_from_plan, pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::Query;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-ensure-sparse-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    // Parent with many outs; spend only vout 3.
    let n_out = 64u32;
    let parent_tx = TxRecord {
        txid: [0xab; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: n_out,
    };
    let parent_outs: Vec<_> = (0..n_out)
        .map(|i| OutputRecord::unspent(1000 + i as i64, vec![0x51, i as u8]))
        .collect();
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let pfk = q
        .store()
        .txs
        .put_full_batch_indexed(&[(parent_tx.clone(), parent_ins, parent_outs)], true)
        .unwrap()[0];

    let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
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
    plan.packed = vec![(
        std::sync::Arc::new((spend_tx, spend_outs)),
        vec![InputRecord {
            prev_txid: parent_tx.txid,
            create_fk: pfk,
            prev_index: 3,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }],
    )];
    plan.planned_fks = vec![Fk(2)];

    let st = ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();
    assert!(st.cold >= 1, "must cold-load multi-out parent: {st:?}");
    let pin = plan
        .external_parent_outs
        .get(&pfk.get().unwrap())
        .expect("sparse external pin");
    assert_eq!(
        pin.1.len(),
        1,
        "must not expand to full output_count={}",
        n_out
    );
    assert_eq!(pin.1[0].0, 3, "only spent need-vout");
    assert_eq!(pin.1[0].1.value, 1003);

    let (parents, _, _) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .unwrap();
    assert!(parents.get_parent_out(Fk(pfk.get().unwrap()), 3).is_some());
    assert!(parents.get_parent_out(Fk(pfk.get().unwrap()), 0).is_none());

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
                version: Version::ONE,
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
                version: Version::ONE,
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
        let arcs = [(Height(h_s0), Arc::new(b_s0.clone()))];
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
        let arcs = [(Height(h_s1), Arc::new(b_s1.clone()))];
        let stamped = confirm_wire_lookup_stamp(&q, &params, ms, &arcs, None).expect("S1 lookup");
        assert!(stamped.plan.is_none(), "S1 already-archived → plan=None");
        let mat =
            confirm_wire_load_from_plan(&q, &params, ms, stamped, None, &ScriptPreverified::new())
                .expect("S1 plan=None load");
        let ok = confirm_scripts_phase(mat.batch).expect("S1 scripts");
        confirm_write_phase(&q, &params, ms, ok.batch).expect("S1 write");
    }
    assert_eq!(q.tip_height().map(|h| h.0), Some(h_s1));

    // Structural: lookup stage source must not denserels-decode body on stamp path.
    // Stamp/load live in lookup.rs after Q-10/Q-11 stage split.
    let src = include_str!("lookup.rs");
    let stamp_fn = src
        .split("pub fn confirm_wire_lookup_stamp")
        .nth(1)
        .and_then(|s| s.split("pub fn confirm_wire_load_from_plan").next())
        .expect("stamp fn slice");
    assert!(
        !stamp_fn.contains("get_outs_by_range_batch"),
        "lookup stamp must never body denserels-decode"
    );
    assert!(
        !stamp_fn.contains("IdxBodyMode::OutsDenserels"),
        "lookup stamp must never idx denserels body"
    );
    // pin_for_wire_batch lives in pin.rs (Q-11 extract); still denserels-by-range only.
    let pin_src = include_str!("pin.rs");
    let load_pin = pin_src
        .split("fn pin_for_wire_batch")
        .nth(1)
        .and_then(|s| s.split("fn ensure_spend_abs_layouts").next())
        .expect("pin fn slice");
    assert!(
        !load_pin.contains("get_fk_by_txid("),
        "load pin must not probe head"
    );
    assert!(
        !load_pin.contains(".body_txid("),
        "load pin must not read txid.body"
    );
    assert!(
        !load_pin.contains("load_creates_once"),
        "load pin must not idx denserels via load_creates_once"
    );
    assert!(
        load_pin.contains("get_outs_by_range_batch"),
        "load pin must denserels by known body range"
    );

    let _ = std::fs::remove_dir_all(&path);
}

/// Load miss: spend edges without pin denserels must hard-fail (no cold tier).
#[test]
fn post_commit_missing_denserels_is_invariant_error() {
    use super::{post_commit, Prepared};
    use crate::milestone::Milestone;
    use crate::params::ChainParams;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{BatchParents, Query};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-post-commit-inv-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();
    // Spend index on (default for Direct) so post_commit enters annotate.
    let _ = (ChainParams::regtest(), Milestone::NONE);

    let prepared = [Prepared {
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
    // Empty BatchParents → get_spender_abs is None.
    let bp = BatchParents::new();
    let meta = rbitcoin_query::U64Map::default();
    let err = post_commit(&q, &prepared, &bp, &meta).expect_err("missing denserels");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant") && msg.contains("denserels"),
        "unexpected err: {msg}"
    );
    let _ = std::fs::remove_dir_all(&path);
}

/// W3: pin already has denserels — ensure only attaches body_range (no denserels cold).
#[test]
fn ensure_range_only_when_pin_has_denserels_skips_cold_body() {
    use super::{ensure_spend_abs_layouts, Prepared};
    use crate::confirm_phase_stats;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{BatchParents, Query};
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::atomic::Ordering;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-ensure-range-only-{}-{}",
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
        txid: [0x11u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let parent_outs = vec![OutputRecord::unspent(50, vec![0x51])];
    let fks = q
        .store()
        .put_tx_full_batch_indexed(
            &[(parent_tx.clone(), parent_ins, parent_outs.clone())],
            /*index=*/ true,
        )
        .unwrap();
    let parent_fk = fks[0];
    let (spent_off, spent_len) = q.store().tx_spent_range(parent_fk).unwrap();

    // Pin without spent_range (load-ahead shape before commit).
    let mut bp = BatchParents::new();
    bp.insert_owned(
        parent_fk,
        parent_tx,
        vec![(0, parent_outs[0].clone())],
        vec![0],
        Some(true),
        None,
        Vec::new(),
    );
    assert!(!bp.has_abs_layout(parent_fk));

    let prepared = [Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(2)],
        jobs: vec![],
        spends: vec![([0x11u8; 32], 0, Fk(2), parent_fk)],
        fees: 0,
        check_scripts: false,
        time: 1,
        bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
        hash: [4u8; 32],
        prev_mtp: 0,
    }];

    let _ = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
    let _ = confirm_phase_stats::ENSURE_RES_HIT.swap(0, Ordering::Relaxed);
    ensure_spend_abs_layouts(&q, &mut bp, &prepared).expect("spent-range ensure");
    let cold = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
    assert_eq!(
        cold, 0,
        "must not denserels-body cold when spent idx stamps abs"
    );
    assert!(bp.has_abs_layout(parent_fk));
    assert_eq!(
        bp.get_spender_abs(parent_fk, 0),
        Some(rbitcoin_store::spent_abs(spent_off, 0))
    );
    let _ = spent_len;
    let _ = std::fs::remove_dir_all(&path);
}

/// Load pin of an already-archived parent stamps `spent_range` (idx only).
/// Write ensure must be a pin-hit for that fk (no second spent.idx batch).
#[test]
fn load_pin_stamps_spent_range_for_archived_parent() {
    use super::{
        ensure_external_parent_denserels_from_plan, ensure_spend_abs_layouts, pin_for_wire_batch,
        ParentPinStamp, Prepared,
    };
    use crate::confirm_phase_stats;
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::Query;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::atomic::Ordering;
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-load-stamp-spent-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&path);
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let parent_tx = TxRecord {
        txid: [0x11u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let parent_outs = vec![OutputRecord::unspent(50, vec![0x51])];
    let parent_ins = vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])];
    let pfk = q
        .store()
        .put_tx_full_batch_indexed(
            &[(parent_tx.clone(), parent_ins, parent_outs)],
            /*index=*/ true,
        )
        .unwrap()[0];
    let (spent_off, spent_len) = q.store().tx_spent_range(pfk).unwrap();
    let expect_abs = rbitcoin_store::spent_abs(spent_off, 0);
    let _ = spent_len;

    let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
    let spend_tx = TxRecord {
        txid: [0x22u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let spend_outs = vec![OutputRecord::unspent(1, vec![0x51])];
    plan.packed = vec![(
        std::sync::Arc::new((spend_tx, spend_outs)),
        vec![InputRecord {
            prev_txid: parent_tx.txid,
            create_fk: pfk,
            prev_index: 0,
            sequence: u32::MAX,
            script_sig: vec![],
            witness: vec![],
        }],
    )];
    plan.planned_fks = vec![Fk(2)];
    ensure_external_parent_denserels_from_plan(&q, Some(&mut plan), None).unwrap();

    let (mut parents, _thin, _warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .unwrap();
    assert!(parents.has_abs_layout(pfk), "load must stamp spent_range");
    assert_eq!(parents.get_spender_abs(pfk, 0), Some(expect_abs));

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
    let _ = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
    ensure_spend_abs_layouts(&q, &mut parents, &prepared).expect("ensure pin-hit");
    let cold = confirm_phase_stats::ENSURE_COLD_N.swap(0, Ordering::Relaxed);
    assert_eq!(
        cold, 0,
        "archived parent must not cold-load at write ensure"
    );
    assert_eq!(parents.get_spender_abs(pfk, 0), Some(expect_abs));
    let _ = std::fs::remove_dir_all(&path);
}

/// Same-batch planned create is not in `spent.idx` yet — load must not invent abs.
#[test]
fn load_pin_does_not_stamp_same_batch_create() {
    use super::{pin_for_wire_batch, ParentPinStamp};
    use rbitcoin_primitives::Fk;
    use rbitcoin_query::Query;
    use rbitcoin_store::{InputRecord, OutputRecord, TxRecord};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-load-no-stamp-same-batch-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let _ = std::fs::remove_dir_all(&path);
    let q = Query::open_or_create(&path).unwrap();

    let mut plan = rbitcoin_query::ArchiveWritePlan::empty();
    let parent_tx = TxRecord {
        txid: [0x22u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    let child_tx = TxRecord {
        txid: [0x33u8; 32],
        version: 1,
        locktime: 0,
        input_start_fk: Fk::NULL,
        input_count: 1,
        output_start_fk: Fk::NULL,
        output_count: 1,
    };
    plan.packed = vec![
        (
            std::sync::Arc::new((parent_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
            vec![InputRecord::coinbase(u32::MAX, vec![0x01], vec![])],
        ),
        (
            std::sync::Arc::new((child_tx, vec![OutputRecord::unspent(1, vec![0x51])])),
            vec![InputRecord {
                prev_txid: [0x22u8; 32],
                create_fk: Fk(2),
                prev_index: 0,
                sequence: u32::MAX,
                script_sig: vec![],
                witness: vec![],
            }],
        ),
    ];
    plan.planned_fks = vec![Fk(2), Fk(3)];

    let (parents, _thin, _warm) = pin_for_wire_batch(
        &q,
        Some(&plan),
        &ParentPinStamp::from_plan(&plan),
        &[],
        &[],
        None,
        None,
    )
    .unwrap();
    assert!(parents.contains(Fk(2)));
    assert!(
        !parents.has_abs_layout(Fk(2)),
        "same-batch create must not get a spent_range before Class A commit"
    );
    assert!(parents.get_spender_abs(Fk(2), 0).is_none());
    let _ = std::fs::remove_dir_all(&path);
}

/// Write-stage ensure must hard-fail when denserels/abs cannot be completed
/// (no silent leave-for structural cold or post_commit).
#[test]
fn ensure_spend_abs_incomplete_is_invariant_error() {
    use super::{ensure_spend_abs_layouts, Prepared};
    use rbitcoin_primitives::{Fk, Height};
    use rbitcoin_query::{BatchParents, Query};
    use std::sync::Once;

    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
            std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
        }
    });
    let path = std::env::temp_dir().join(format!(
        "rbitcoin-ensure-abs-inv-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    std::fs::create_dir_all(&path).unwrap();
    let q = Query::open_or_create(&path).unwrap();
    q.enter_direct_index_mode().unwrap();

    let prepared = [Prepared {
        height: Height(1),
        header_fk: Fk(1),
        tx_fks: vec![Fk(10)],
        jobs: vec![],
        // Non-null create_fk that does not exist in Class A → cold load miss.
        spends: vec![([9u8; 32], 0, Fk(10), Fk(999_999))],
        fees: 0,
        check_scripts: false,
        time: 1,
        bits: bitcoin::CompactTarget::from_consensus(0x207f_ffff),
        hash: [3u8; 32],
        prev_mtp: 0,
    }];
    let mut bp = BatchParents::new();
    let err = ensure_spend_abs_layouts(&q, &mut bp, &prepared)
        .expect_err("ensure must hard-fail without denserels");
    let msg = format!("{err}");
    assert!(
        msg.contains("invariant")
            && (msg.contains("ensure denserels") || msg.contains("abs incomplete")),
        "unexpected err: {msg}"
    );
    let _ = std::fs::remove_dir_all(&path);
}

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
            version: Version::ONE,
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

#[test]
fn write_mtp_does_not_get_header_plan() {
    let mtp = include_str!("../block/mod.rs");
    let start = mtp.find("fn mtp_at(").expect("mtp_at");
    let body = &mtp[start..start + 600];
    assert!(
        body.contains("median_time_past_store"),
        "write mtp_at must use store+carried, not header-cache median_time_past"
    );
    assert!(
        !body.contains("median_time_past(query"),
        "write mtp_at must not call get_header_plan via median_time_past"
    );
    let phases = include_str!("phases.rs");
    let post = phases
        .split("pub(super) fn post_commit")
        .nth(1)
        .expect("post_commit");
    assert!(
        !post.contains("advance_parent_cache_tip"),
        "post_commit must not GC header cache; load polls store tip"
    );
    let write = include_str!("write.rs");
    assert!(
        !write.contains("advance_parent_cache_tip"),
        "write must not call advance_parent_cache_tip"
    );
    assert!(
        write.contains("note_head_drain_fk"),
        "write publishes drain-fk HWM after insert"
    );
    assert!(
        !write.contains("send_leftover_notes"),
        "write does not send leftover notes (Class A does)"
    );
}
