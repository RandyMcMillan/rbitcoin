//! tests (peeled from ibd/confirm.rs).

use super::{
    format_conf_q, format_queue_depth, format_stamp_reject_missing_prevout,
    stamp_reject_operator_msg, ConfirmFeed, ConfirmQueueDepths,
};
use bitcoin::hashes::Hash;
use bitcoin::BlockHash;
use rbitcoin_primitives::Fk;
use rbitcoin_query::{InFlightLayer, InFlightLog};
use rbitcoin_store::{OutputRecord, TxRecord};
use std::sync::Arc;

fn test_pin(id: u64) -> rbitcoin_query::CreatePin {
    let mut txid = [0u8; 32];
    txid[..8].copy_from_slice(&id.to_le_bytes());
    Arc::new((
        TxRecord {
            txid,
            version: 1,
            locktime: 0,
            input_start_fk: Fk::NULL,
            input_count: 0,
            output_start_fk: Fk::NULL,
            output_count: 0,
        },
        vec![OutputRecord::unspent(1, vec![0x51])],
    ))
}

/// Unconfirmed pack stays after head drain; drop only once the fence covers it.
///
/// Occupied/confirmed-HWM prune dropped tip-ahead parents (931147 / 945952).
#[test]
fn prune_inflight_keeps_until_tip_covers_height() {
    let mut log = InFlightLog::new();
    let pins: Vec<_> = (85u64..=100).map(|id| (Fk(id), test_pin(id))).collect();
    log.note_layer(
        InFlightLayer::from_plan_pins(pins.iter().map(|(f, p)| (*f, p))).with_max_height(10),
    );
    log.prune_through_tip(Some(9));
    assert_eq!(log.entry_count(), 16, "tip < max_height keeps the pack");
    log.prune_through_tip(Some(10));
    assert_eq!(log.layer_count(), 0);
}

/// Mainnet 187: first pack writes (drain+fence), next pack spends those creates.
/// Stamp skips body_range when in-flight still has CreatePin outs; pin needs
/// those outs. Drive the shipped confirm engine (not a source-order pin).
#[test]
fn confirm_engine_pins_spend_of_just_written_pack() {
    use super::{spawn_confirm_engine, ConfirmEvent, ConfirmFeed};
    use crate::chain::ChainHub;
    use crate::ibd::status::LoopStats;
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Txid, Witness};
    use rbitcoin_consensus::{mine_regtest_paying, pad_empty_from, ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::sync::atomic::AtomicU32;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    if std::env::var_os("RBITCOIN_HEAD_SCALE").is_none() {
        std::env::set_var("RBITCOIN_HEAD_SCALE", "tiny");
    }
    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-engine-187-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    q.enter_direct_index_mode().unwrap();
    let params = ChainParams::regtest();
    let hub = Arc::new(ChainHub::new(q, params.clone(), Milestone::NONE));
    hub.ensure_genesis().unwrap();
    let genesis = hub.tip_hash().expect("genesis");
    let gen_time = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest)
        .header
        .time;
    let maturity = params.coinbase_maturity();
    let (tip, tip_time, cbs) =
        pad_empty_from(&hub.query, &params, genesis, gen_time, 1, maturity + 1, 1);
    let matured = cbs[0];
    let spend = |prev: Txid, val: Amount| Transaction {
        version: TxVersion::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: prev,
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: val,
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    };
    let h_parent = maturity + 2;
    let parent = mine_regtest_paying(
        tip,
        tip_time + 600,
        h_parent,
        ScriptBuf::from_bytes(vec![0x51]),
        vec![spend(matured, Amount::from_sat(49_0000_0000))],
    );
    let parent_spend_txid = parent.txdata[1].compute_txid();
    let child = mine_regtest_paying(
        parent.block_hash(),
        parent.header.time + 600,
        h_parent + 1,
        ScriptBuf::from_bytes(vec![0x51]),
        vec![spend(parent_spend_txid, Amount::from_sat(48_0000_0000))],
    );
    let child_h = h_parent + 1;
    let child_hash = child.block_hash();

    let feed = Arc::new(ConfirmFeed::new());
    let (ev_tx, ev_rx) = std::sync::mpsc::channel();
    let accepted = Arc::new(AtomicU32::new(0));
    let (engine, _queues) = spawn_confirm_engine(
        Arc::clone(&hub),
        Arc::clone(&feed),
        ev_tx,
        accepted,
        Arc::new(LoopStats::default()),
    );

    let wait_tip = |want: u32| {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if hub.tip_height() == Some(want) {
                return;
            }
            match ev_rx.recv_timeout(Duration::from_millis(50)) {
                Ok(ConfirmEvent::Reject { height, err, .. }) => {
                    panic!("confirm reject @{height}: {err}");
                }
                Ok(_) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    if Instant::now() > deadline {
                        panic!(
                            "timeout waiting for tip={want} (have {:?})",
                            hub.tip_height()
                        );
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    panic!(
                        "confirm engine exited before tip={want} (have {:?})",
                        hub.tip_height()
                    );
                }
            }
        }
    };

    // Two packs: write parent (drain+fence) before the child is even offered.
    // Production path is BQ raw → lookup take → loadq (not feed.note_wire).
    use bitcoin::consensus::encode::serialize;
    hub.query
        .block_queue_enqueue(
            h_parent,
            parent.block_hash().to_byte_array(),
            1,
            &serialize(&parent),
        )
        .unwrap();
    feed.note(h_parent, parent.block_hash());
    wait_tip(h_parent);
    hub.query
        .block_queue_enqueue(child_h, child_hash.to_byte_array(), 1, &serialize(&child))
        .unwrap();
    feed.note(child_h, child_hash);
    wait_tip(child_h);

    feed.request_stop();
    feed.notify();
    let _ = engine.join();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Drain can lead fence; tip prune must still keep the unconfirmed height.
#[test]
fn prune_inflight_keeps_unconfirmed_after_occupied_jumps() {
    let mut log = InFlightLog::new();
    let p = test_pin(42);
    log.note_layer(InFlightLayer::from_plan_pins([(Fk(42), &p)]).with_max_height(1));
    log.prune_through_tip(Some(0));
    assert!(
        log.snapshot().get_create_fk(&p.0.txid).is_some(),
        "occupied/fence lag must not drop height > tip"
    );
}

/// Lookup may note new packs while load holds a prior snapshot — prior Arc
/// layers must stay frozen (no whole-map make_mut).
#[test]
fn note_while_prep_holds_snapshot_does_not_clone_prior_layers() {
    let mut log = InFlightLog::new();
    let p1 = test_pin(1);
    log.note_layer(InFlightLayer::from_plan_pins([(Fk(1), &p1)]));
    let held = log.snapshot();
    for i in 2u64..=30 {
        let p = test_pin(i);
        log.note_layer(InFlightLayer::from_plan_pins([(Fk(i), &p)]));
    }
    assert_eq!(held.layer_count(), 1);
    assert!(held.get_out(1).is_some());
    assert!(held.get_out(30).is_none());
    assert_eq!(log.layer_count(), 30);
    assert!(log.snapshot().get_out(30).is_some());
}

fn bh(b: u8) -> BlockHash {
    BlockHash::from_byte_array([b; 32])
}

/// Contiguous feed claim from `expect`, optional skip of already-confirmed.
fn claim_feed_run(
    expect: u32,
    max: usize,
    claim_hi: u32,
    feed_has: impl Fn(u32) -> bool,
    already_confirmed: impl Fn(u32) -> bool,
) -> Vec<u32> {
    let mut run = Vec::with_capacity(max.min(32));
    let mut h = expect;
    while run.len() < max && h <= claim_hi {
        if !feed_has(h) {
            break;
        }
        if already_confirmed(h) {
            h = h.saturating_add(1);
            continue;
        }
        run.push(h);
        h = h.saturating_add(1);
    }
    run
}

/// Offline mirror of online pack: prefix length under soft inputs + hard blocks.
fn pack_confirm_run_len(
    input_counts: &[u32],
    soft_max_inputs: u32,
    hard_max_blocks: usize,
) -> usize {
    if input_counts.is_empty() || hard_max_blocks == 0 {
        return 0;
    }
    let mut sum = 0u32;
    let mut n = 0usize;
    for &c in input_counts {
        sum = sum.saturating_add(c);
        n += 1;
        if super::pack_stop_after(sum, n, soft_max_inputs, hard_max_blocks) {
            break;
        }
    }
    n.max(1).min(input_counts.len())
}

#[test]
fn pack_confirm_run_len_policy() {
    use super::{CONFIRM_BATCH_INPUTS_DEFAULT, CONFIRM_RUN_MAX_BLOCKS};
    // Under budget: take all.
    assert_eq!(pack_confirm_run_len(&[10, 10, 10], 8000, 144), 3);
    // Soft overshoot: include crossing block then stop.
    // 7990 + 100 = 8090 > 8000 → n=2
    assert_eq!(pack_confirm_run_len(&[7990, 100, 50], 8000, 144), 2);
    // First block alone exceeds soft → n=1
    assert_eq!(pack_confirm_run_len(&[50_000, 10], 8000, 144), 1);
    // Block hard cap
    let ones = vec![1u32; 200];
    assert_eq!(
        pack_confirm_run_len(&ones, CONFIRM_BATCH_INPUTS_DEFAULT, CONFIRM_RUN_MAX_BLOCKS),
        CONFIRM_RUN_MAX_BLOCKS
    );
    assert_eq!(pack_confirm_run_len(&[], 8000, 144), 0);
    // Exactly at soft: sum==soft continues? policy is sum > soft stop after take.
    // 4000+4000=8000 not > 8000 → can take more if present
    assert_eq!(pack_confirm_run_len(&[4000, 4000, 1], 8000, 144), 3);
    // After third, sum=8001 > 8000 stops at 3
    assert_eq!(pack_confirm_run_len(&[4000, 4000, 1, 1], 8000, 144), 3);
}

#[test]
fn split_wave_into_load_batches_is_eight_by_8000() {
    use super::{
        split_wave_into_load_batches_kind, CONFIRM_BATCH_INPUTS_DEFAULT, CONFIRM_RUN_MAX_BLOCKS,
        LOAD_QUEUE_CAP_DEFAULT,
    };
    assert_eq!(LOAD_QUEUE_CAP_DEFAULT, 14);
    assert_eq!(super::confirm_queue_caps().load, LOAD_QUEUE_CAP_DEFAULT);
    assert_eq!(super::load_queue_cap(), LOAD_QUEUE_CAP_DEFAULT);
    assert!(super::LoadBatch { items: vec![] }.items.is_empty());
    // 8 × 8001 inputs (each block overshoots 8000) → 8 batches of one.
    let wave: Vec<u32> = vec![8001; 8];
    let parts = split_wave_into_load_batches_kind(
        &wave,
        &[],
        CONFIRM_BATCH_INPUTS_DEFAULT,
        CONFIRM_RUN_MAX_BLOCKS,
    );
    assert_eq!(parts, vec![1, 1, 1, 1, 1, 1, 1, 1]);
    // Exactly 8000 does not stop; two 8000-input blocks are one batch.
    assert_eq!(
        split_wave_into_load_batches_kind(&[8000, 8000], &[], 8000, 144),
        vec![2]
    );
    // Empty / single megablock.
    assert!(split_wave_into_load_batches_kind(&[], &[], 8000, 144).is_empty());
    assert_eq!(
        split_wave_into_load_batches_kind(&[50_000], &[], 8000, 144),
        vec![1]
    );
    // 144 thin blocks then 144 more → two hard-cap batches.
    let thin = vec![1u32; 288];
    assert_eq!(
        split_wave_into_load_batches_kind(&thin, &[], 8000, 144),
        vec![144, 144]
    );
}

#[test]
fn split_wave_into_load_batches_stops_at_has_body_change() {
    use super::split_wave_into_load_batches_kind;
    // Crash prefix already-bodied, suffix need-body: two batches.
    let counts = [1u32, 1, 1, 1, 1];
    let has_body = [true, true, false, false, false];
    assert_eq!(
        split_wave_into_load_batches_kind(&counts, &has_body, 8000, 144),
        vec![2, 3]
    );
    // Kind flip inside an 8000-input pack still splits (do not glue kinds).
    assert_eq!(
        split_wave_into_load_batches_kind(&[4000, 4000], &[true, false], 8000, 144),
        vec![1, 1]
    );
    // Homogeneous still packs on input cap only.
    assert_eq!(
        split_wave_into_load_batches_kind(&[8000, 8000], &[false, false], 8000, 144),
        vec![2]
    );
    assert!(split_wave_into_load_batches_kind(&[], &[], 8000, 144).is_empty());
    assert_eq!(
        split_wave_into_load_batches_kind(&[50_000], &[true], 8000, 144),
        vec![1]
    );
}

#[test]
fn load_recv_is_lookup_order() {
    use super::LoadBatch;
    use rbitcoin_query::{ResolvedWire, TxPrecompute};
    use std::sync::mpsc;
    use std::sync::Arc;
    let (tx, rx) = mpsc::sync_channel::<LoadBatch>(8);
    let mk = |h: u32| {
        let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
        let pres: Arc<[TxPrecompute]> = genesis
            .txdata
            .iter()
            .map(TxPrecompute::from_tx)
            .collect::<Vec<_>>()
            .into();
        (
            h,
            [h as u8; 32],
            ResolvedWire {
                block: Arc::new(genesis),
                pres,
            },
        )
    };
    tx.send(LoadBatch {
        items: vec![mk(1), mk(2)],
    })
    .unwrap();
    tx.send(LoadBatch { items: vec![mk(3)] }).unwrap();
    let a = rx.recv().unwrap();
    let b = rx.recv().unwrap();
    assert_eq!(a.items[0].0, 1);
    assert_eq!(a.items[1].0, 2);
    assert_eq!(b.items[0].0, 3);
}

#[test]
fn load_stamp_items_keep_pres() {
    use super::{load_stamp_items, LoadBatch};
    use rbitcoin_query::{ResolvedWire, TxPrecompute};
    let genesis = bitcoin::blockdata::constants::genesis_block(bitcoin::Network::Regtest);
    let pres: Arc<[TxPrecompute]> = genesis
        .txdata
        .iter()
        .map(TxPrecompute::from_tx)
        .collect::<Vec<_>>()
        .into();
    let lb = LoadBatch {
        items: vec![(
            1,
            [1u8; 32],
            ResolvedWire {
                block: Arc::new(genesis),
                pres: Arc::clone(&pres),
            },
        )],
    };
    let items = load_stamp_items(lb.items.into_iter().map(|(h, _, w)| (h, w.block, w.pres)));
    assert_eq!(items.len(), 1);
    let got = items[0].2.as_ref().expect("load must pass lookup pres");
    assert!(
        Arc::ptr_eq(got, &pres),
        "stamp input must keep the LoadBatch pres Arc"
    );
}

#[test]
fn lookup_blocks_when_loadq_full() {
    use super::{LoadBatch, LOAD_QUEUE_CAP_DEFAULT};
    use std::sync::mpsc;
    let (tx, rx) = mpsc::sync_channel::<LoadBatch>(LOAD_QUEUE_CAP_DEFAULT);
    for _ in 0..LOAD_QUEUE_CAP_DEFAULT {
        tx.send(LoadBatch { items: vec![] }).unwrap();
    }
    assert!(
        tx.try_send(LoadBatch { items: vec![] }).is_err(),
        "9th send must wait / fail while loadq is full"
    );
    let _ = rx.recv().unwrap();
    tx.send(LoadBatch { items: vec![] }).unwrap();
}

#[test]
fn block_input_count_sums_tx_inputs() {
    use bitcoin::absolute::LockTime;
    use bitcoin::block::{Header, Version};
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{
        Amount, Block, CompactTarget, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness,
    };
    let mk_tx = |n_in: usize| Transaction {
        version: TxVersion::ONE,
        lock_time: LockTime::ZERO,
        input: (0..n_in)
            .map(|i| TxIn {
                previous_output: OutPoint {
                    txid: bitcoin::Txid::from_byte_array([i as u8; 32]),
                    vout: 0,
                },
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: Witness::new(),
            })
            .collect(),
        output: vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::new(),
        }],
    };
    let header = Header {
        version: Version::from_consensus(4),
        prev_blockhash: BlockHash::from_byte_array([0; 32]),
        merkle_root: bitcoin::TxMerkleNode::from_byte_array([0; 32]),
        time: 1,
        bits: CompactTarget::from_consensus(0x207fffff),
        nonce: 0,
    };
    let block = Block {
        header,
        txdata: vec![mk_tx(1), mk_tx(3), mk_tx(2)],
    };
    assert_eq!(super::block_input_count(&block), 6);
}

/// Parent entry meters accumulate and drain with send/recv (no budget gate).
#[test]
fn pipeline_parents_meter_prep_and_write() {
    let q = ConfirmQueueDepths::new();
    q.note_script_send(2, 1_000, 50);
    q.note_write_send(3, 2_000, 80);
    let c = q.content_snap();
    assert_eq!(c.script_parents, 50);
    assert_eq!(c.write_parents, 80);
    assert_eq!(c.parents_total(), 130);
    q.note_script_recv(2, 1_000, 50);
    q.note_write_recv(3, 2_000, 80);
    let c2 = q.content_snap();
    assert_eq!(c2.parents_total(), 0);
    // Over-recv saturates at 0.
    q.note_write_recv(1, 1, 99);
    assert_eq!(q.content_snap().write_parents, 0);
}

/// Contiguous claim + skip already-confirmed (pure claim helper).
#[test]
fn claim_feed_wave_and_skip_confirmed() {
    let run = claim_feed_run(101, 32, 200, |h| h >= 101 && h < 101 + 40, |_| false);
    assert_eq!(run.len(), 32);
    assert_eq!(run[0], 101);
    assert_eq!(*run.last().unwrap(), 132);
    let run = claim_feed_run(10, 32, 200, |h| h >= 10 && h <= 50, |h| h == 10 || h == 11);
    assert_eq!(run.first().copied(), Some(12));
    assert_eq!(run.len(), 32);
}

/// Claim must not jump thousands past tip when near pipeline is full.
#[test]
fn claim_ahead_cap_blocks_far_skip() {
    let ahead = super::max_claim_ahead();
    assert!(ahead >= super::CONFIRM_RUN_MAX_BLOCKS as u32);
    assert!(
        ahead
            <= 64 * 3 * super::CONFIRM_RUN_MAX_BLOCKS as u32 + super::CONFIRM_RUN_MAX_BLOCKS as u32,
        "keep claim window within env clamp: {ahead}"
    );
    let path_lo = 87u32;
    let run = claim_feed_run(
        path_lo,
        super::CONFIRM_RUN_MAX_BLOCKS,
        path_lo + ahead,
        |h| h >= path_lo && h < path_lo + 1000,
        |_| false,
    );
    assert_eq!(run.len(), super::CONFIRM_RUN_MAX_BLOCKS);
    assert_eq!(run[0], path_lo);
    assert!(*run.last().unwrap() <= path_lo + ahead);
}

/// requeue_wire after empty load must clear inflight (Ok(None) leak regression).
#[test]
fn requeue_clears_inflight_so_tip_can_retry() {
    let feed = ConfirmFeed::new();
    feed.note(87, bh(1));
    feed.note(88, bh(2));
    {
        let mut g = feed.inner.lock().unwrap();
        g.ready.remove(&87);
        g.ready.remove(&88);
        g.inflight.insert(87);
        g.inflight.insert(88);
    }
    feed.requeue_wire(&[(87, bh(1), None), (88, bh(2), None)]);
    let g = feed.inner.lock().unwrap();
    assert!(!g.inflight.contains(&87));
    assert!(!g.inflight.contains(&88));
    assert!(g.ready.contains_key(&87));
    assert!(g.ready.contains_key(&88));
}

#[test]
fn queue_hwm_tracks_max_depth() {
    let q = ConfirmQueueDepths::new();
    q.note_script_send(32, 1, 0);
    q.note_script_send(32, 1, 0);
    assert_eq!(q.snap().1, 2);
    q.note_script_recv(32, 1, 0);
    assert_eq!(q.snap().1, 1);
    let (_lh, sh, wh) = q.sample_hwm_and_reset();
    assert_eq!(sh, 2, "hwm keeps max even after recv");
    assert_eq!(wh, 0);
    let (_, sh2, _) = q.sample_hwm_and_reset();
    assert_eq!(sh2, 0, "hwm resets each sample window");
}

/// Debug overflow on script_wire_bytes / parents used to abort IBD confirm
/// threads under parallel load (seen on two_node IBD). Counters must saturate.
#[test]
fn queue_load_send_saturates_wire_and_parents() {
    let q = ConfirmQueueDepths::new();
    // Near-max wire_bytes so a second large add would wrap without saturating.
    let half = usize::MAX / 2 + 1;
    q.note_script_send(1, half, half);
    q.note_script_send(1, half, half);
    let c = q.content_snap();
    assert_eq!(c.script_wire_bytes, usize::MAX);
    assert_eq!(c.script_parents, usize::MAX);
    assert_eq!(c.script_blocks, 2);
    // recv must not underflow
    q.note_script_recv(1, half, half);
    let c2 = q.content_snap();
    assert!(c2.script_wire_bytes <= usize::MAX);
    assert!(c2.script_parents <= usize::MAX);
}

#[test]
fn thr_stats_add_is_local() {
    use super::confirm_thr_stats;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;
    let a = AtomicU64::new(0);
    confirm_thr_stats::add(&a, Duration::from_millis(5));
    confirm_thr_stats::add(&a, Duration::from_millis(20));
    assert!(a.load(Ordering::Relaxed) >= 25_000_000);
    let before = a.load(Ordering::Relaxed);
    confirm_thr_stats::add(&a, Duration::ZERO);
    assert_eq!(
        a.load(Ordering::Relaxed),
        before,
        "zero duration is a no-op"
    );
    assert_eq!(
        confirm_thr_stats::script_work_from_verify_ns(2_000),
        Duration::from_nanos(2_000)
    );
}

#[test]
fn stamp_reject_names_leftover_unresolved() {
    let msg = stamp_reject_operator_msg("missing prevout");
    assert!(msg.contains("missing prevout"), "{msg}");
    assert!(msg.contains("unresolved"), "{msg}");
    assert!(msg.contains("leftover_n="), "{msg}");
    assert!(msg.contains("leftover_hit="), "{msg}");
    assert!(
        !msg.contains("corrupt"),
        "must not look like store wipe: {msg}"
    );
    assert_eq!(
        stamp_reject_operator_msg("unexpected previous header"),
        "unexpected previous header"
    );
}

/// 257581: leftover_n/hit is not enough — we need the missing prev_txid and
/// whether write-behind still holds it (TipOnly is durable head only).
#[test]
fn stamp_reject_names_union_miss_txid() {
    let mut raw = [0u8; 32];
    raw[0] = 0xab;
    raw[31] = 0xcd;
    let msg =
        format_stamp_reject_missing_prevout(1914, 1913, 1, Some(raw), true, Some("head"), 0, false);
    assert!(msg.contains("leftover_n=1914"), "{msg}");
    assert!(msg.contains("leftover_hit=1913"), "{msg}");
    assert!(msg.contains("miss_n=1"), "{msg}");
    assert!(msg.contains("miss_txid="), "{msg}");
    assert!(msg.contains("pending=1"), "{msg}");
    assert!(msg.contains("miss_on=head"), "{msg}");
    assert!(msg.contains("miss_cands=0"), "{msg}");
    let disp = bitcoin::Txid::from_byte_array(raw).to_string();
    assert!(
        msg.contains(&disp),
        "operator line must name display txid {disp}: {msg}"
    );
}

/// note / requeue / finish lifecycle (duplicate scripts bug + re-queue).
#[test]
fn feed_note_requeue_finish_surface() {
    let feed = ConfirmFeed::new();
    feed.note(100, bh(1));
    {
        let mut g = feed.inner.lock().unwrap();
        let (hash, wire) = g.ready.remove(&100).unwrap();
        g.inflight.insert(100);
        assert_eq!(hash, bh(1));
        assert!(wire.is_none());
    }
    // Main loop offer would re-note tip+1 every tick — must be ignored.
    feed.note(100, bh(1));
    {
        let g = feed.inner.lock().unwrap();
        assert!(
            g.ready.is_empty(),
            "inflight height must not re-enter ready"
        );
        assert!(g.inflight.contains(&100));
    }

    {
        let mut g = feed.inner.lock().unwrap();
        g.inflight.insert(50);
        g.inflight.insert(51);
    }
    feed.requeue_wire(&[(50, bh(5), None), (51, bh(6), None)]);
    {
        let g = feed.inner.lock().unwrap();
        assert!(!g.inflight.contains(&50));
        assert_eq!(g.ready.get(&50).map(|(h, _)| *h), Some(bh(5)));
        assert_eq!(g.ready.get(&51).map(|(h, _)| *h), Some(bh(6)));
    }

    {
        let mut g = feed.inner.lock().unwrap();
        g.inflight.insert(10);
        g.inflight.insert(11);
    }
    feed.finish([10, 11]);
    let g = feed.inner.lock().unwrap();
    assert!(!g.inflight.contains(&10));
    assert!(!g.inflight.contains(&11));
}

/// Log tokens + live caps (scriptq/writeq; ready= is not capped).
#[test]
fn queue_depth_log_and_caps_surface() {
    assert_eq!(format_queue_depth("write", 0, 2), "write<0/2");
    assert_eq!(format_queue_depth("script", 1, 2), "script=1/2");
    assert_eq!(format_queue_depth("write", 2, 2), "write=2/2");
    assert_eq!(
        format_conf_q(0, 0, 1, 8, 2, 2),
        "loadq<0/8 scriptq<0/2 writeq=1/2"
    );
    assert_eq!(
        format_conf_q(3, 1, 0, 8, 2, 2),
        "loadq=3/8 scriptq=1/2 writeq<0/2"
    );
    assert_eq!(
        format_conf_q(0, 0, 0, 8, 2, 2),
        "loadq<0/8 scriptq<0/2 writeq<0/2"
    );

    let caps = super::confirm_queue_caps();
    assert_eq!(caps.script, super::SCRIPT_QUEUE_CAP_DEFAULT);
    assert_eq!(caps.write, super::WRITE_QUEUE_CAP_DEFAULT);
    assert_eq!(super::script_queue_cap(), caps.script);
    assert_eq!(super::write_queue_cap(), caps.write);
    for c in [caps.script, caps.write] {
        assert!(c >= 1, "queue cap must be positive: {c}");
    }
    assert_eq!(
        format_conf_q(0, 0, 0, caps.load, caps.script, caps.write),
        format!(
            "loadq<0/{} scriptq<0/{} writeq<0/{}",
            caps.load, caps.script, caps.write
        )
    );
    assert_eq!(
        format_conf_q(
            caps.load,
            caps.script,
            caps.write,
            caps.load,
            caps.script,
            caps.write
        ),
        format!(
            "loadq={0}/{0} scriptq={1}/{1} writeq={2}/{2}",
            caps.load, caps.script, caps.write
        )
    );
}

#[test]
fn feed_stop_size_snap_and_empty_requeue() {
    let feed = ConfirmFeed::new();
    assert!(!feed.stopped());
    assert_eq!(feed.size_snap(), (0, 0));
    feed.note(1, bh(1));
    feed.note(2, bh(2));
    {
        let mut g = feed.inner.lock().unwrap();
        g.inflight.insert(3);
    }
    assert_eq!(feed.size_snap(), (2, 1));
    feed.requeue_wire(&[]); // no-op empty
    assert_eq!(feed.size_snap(), (2, 1));
    feed.request_stop();
    assert!(feed.stopped());
}

#[test]
fn claim_feed_stops_at_gap() {
    let run = claim_feed_run(5, 10, 100, |h| h == 5 || h == 6 || h == 8, |_| false);
    // Contiguous only — gap at 7 stops.
    assert_eq!(run, vec![5, 6]);
    let empty = claim_feed_run(1, 8, 100, |_| false, |_| false);
    assert!(empty.is_empty());
}

#[test]
fn confirm_queue_depths_content_snap_and_notes() {
    use super::ConfirmQueueDepths;
    let q = ConfirmQueueDepths::new();
    assert_eq!(q.snap(), (0, 0, 0));
    let c0 = q.content_snap();
    assert_eq!(c0.script_batches, 0);
    assert_eq!(c0.write_batches, 0);
    assert_eq!(c0.feed_ready, 0);
    assert_eq!(c0.feed_inflight, 0);

    q.note_script_send(3, 1000, 2);
    q.note_write_send(2, 500, 7);
    let c1 = q.content_snap();
    assert_eq!(c1.script_batches, 1);
    assert_eq!(c1.script_blocks, 3);
    assert_eq!(c1.script_wire_bytes, 1000);
    assert_eq!(c1.script_parents, 2);
    assert_eq!(c1.write_batches, 1);
    assert_eq!(c1.write_blocks, 2);
    assert_eq!(c1.write_wire_bytes, 500);
    assert_eq!(c1.write_parents, 7);
    assert_eq!(c1.parents_total(), 9);
    assert_eq!(q.snap(), (0, 1, 1));

    q.note_script_recv(3, 1000, 2);
    q.note_write_recv(2, 500, 7);
    let c2 = q.content_snap();
    assert_eq!(c2.script_batches, 0);
    assert_eq!(c2.write_batches, 0);
    assert_eq!(c2.script_blocks, 0);
    assert_eq!(c2.write_blocks, 0);
    // saturating sub: over-recv is safe
    q.note_script_recv(99, 99, 99);
    assert_eq!(q.content_snap().script_blocks, 0);
}

#[test]
fn offer_confirm_ready_walks_height_map() {
    use super::super::body::BodyPresence;
    use super::offer_confirm_ready;
    use rbitcoin_consensus::{ChainParams, Milestone};
    use rbitcoin_query::Query;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU32;

    let dir = std::env::temp_dir().join(format!(
        "rbitcoin-offer-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::create_dir_all(&dir);
    let q = Query::open_or_create(dir.join("store")).unwrap();
    let hub = crate::chain::ChainHub::new(q, ChainParams::regtest(), Milestone::NONE);
    hub.ensure_genesis().unwrap();

    let feed = ConfirmFeed::new();
    let mut body = BodyPresence::new();
    let mut h2h = HashMap::new();
    // Tip is 0; expect tip+1 = 1. Zombie pending without BQ is not claim-ready.
    let h1 = bh(0x11);
    h2h.insert(1u32, h1);
    body.mark_pending(h1);
    let mut max_arch = 0u32;
    let shared = AtomicU32::new(0);
    let n = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
    assert_eq!(n, 0, "pending without body queue must not note");
    assert_eq!(feed.size_snap().0, 0);

    // Rejected tip+1 stops and notes zero new.
    body.mark_rejected(h1);
    let n2 = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
    assert_eq!(n2, 0);

    // Gap in height map stops.
    let h2h2 = HashMap::new();
    let n3 = offer_confirm_ready(&feed, &h2h2, &mut body, &hub, &mut max_arch, &shared);
    assert_eq!(n3, 0);

    // Already-confirmed tip heights are skipped (continue walking).
    // Archive+confirm height 1 so has_block is true; offer from tip=1 expects 2.
    let h2 = bh(0x22);
    // tip is still 0 (genesis only) — mark genesis-next already confirmed via
    // has_block is only true for store tip; exercise the continue arm by
    // re-running offer after feed has height 1 already noted (inflight path).
    feed.note(1, h1); // already ready — note is idempotent when not inflight
    {
        let mut g = feed.inner.lock().unwrap();
        g.inflight.insert(1);
        g.ready.remove(&1);
    }
    // With tip+1 inflight, offer still notes if ready map empty for that height.
    let n4 = offer_confirm_ready(&feed, &h2h, &mut body, &hub, &mut max_arch, &shared);
    // rejected path already cleared h1 from ready; height 1 still rejected → 0.
    assert_eq!(n4, 0);

    // Class A alone is not claim-ready: height 1 archived without bq → offer 0.
    body = BodyPresence::new();
    let mut h2h3 = HashMap::new();
    h2h3.insert(1u32, h1);
    h2h3.insert(2u32, h2);
    body.mark_archived(h1);
    feed.finish([1]);
    max_arch = 0;
    let n5 = offer_confirm_ready(&feed, &h2h3, &mut body, &hub, &mut max_arch, &shared);
    assert_eq!(
        n5, 0,
        "Class A without body queue must not note confirm feed"
    );

    // Zombie pending without BQ is still not claim-ready.
    body.mark_pending(h1);
    let n6 = offer_confirm_ready(&feed, &h2h3, &mut body, &hub, &mut max_arch, &shared);
    assert_eq!(n6, 0, "pending alone must not note without body queue");

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn claim_feed_skips_inflight_and_confirmed_in_helper() {
    // Pure claim helper: inflight-like skip is modeled by already_confirmed.
    // Heights 1..=10 present; skip 1,2,5 → claim 3,4,6,7,8,9,10 (7).
    let run = claim_feed_run(
        1,
        8,
        100,
        |h| (1..=10).contains(&h),
        |h| h == 1 || h == 2 || h == 5,
    );
    assert_eq!(run.first().copied(), Some(3));
    assert!(!run.contains(&5));
    assert_eq!(run, vec![3, 4, 6, 7, 8, 9, 10]);
    // Max 0 → empty.
    assert!(claim_feed_run(1, 0, 100, |_| true, |_| false).is_empty());
}

/// note_wire prefer path (instance-local; not process-global thr_stats).
#[test]
fn thr_stats_all_stages_and_note_wire_prefer() {
    // note_wire: prefer keeping wire when already noted without; ignore inflight.
    let feed = ConfirmFeed::new();
    feed.note(10, bh(1));
    {
        let g = feed.inner.lock().unwrap();
        assert!(g.ready.get(&10).unwrap().1.is_none());
    }
    // Re-note with wire upgrades the optional slot.
    let genesis = rbitcoin_consensus::genesis_block(&rbitcoin_consensus::ChainParams::regtest());
    feed.note_wire(10, bh(1), Some(genesis.clone()));
    {
        let g = feed.inner.lock().unwrap();
        assert!(g.ready.get(&10).unwrap().1.is_some());
    }
    // Second note_wire with wire does not replace existing wire.
    let kept_nonce = genesis.header.nonce;
    let mut other = genesis.clone();
    other.header.nonce = kept_nonce.wrapping_add(99);
    feed.note_wire(10, bh(1), Some(other));
    {
        let g = feed.inner.lock().unwrap();
        assert_eq!(
            g.ready.get(&10).unwrap().1.as_ref().unwrap().header.nonce,
            kept_nonce,
            "must keep first wire, not replace"
        );
    }
    // Inflight height ignores note_wire entirely.
    {
        let mut g = feed.inner.lock().unwrap();
        g.inflight.insert(11);
    }
    feed.note_wire(11, bh(2), Some(genesis.clone()));
    {
        let g = feed.inner.lock().unwrap();
        assert!(!g.ready.contains_key(&11));
    }
    // requeue into existing ready without wire upgrades it.
    feed.requeue_wire(&[(10, bh(1), None)]);
    {
        let g = feed.inner.lock().unwrap();
        assert!(g.ready.get(&10).unwrap().1.is_some());
        assert!(!g.inflight.contains(&10));
    }
    // requeue into existing ready that already has no wire, supply wire.
    feed.note(12, bh(3));
    feed.requeue_wire(&[(12, bh(3), Some(genesis))]);
    {
        let g = feed.inner.lock().unwrap();
        assert!(g.ready.get(&12).unwrap().1.is_some());
    }

    // pack_stop_after edges.
    assert!(!super::pack_stop_after(0, 0, 8000, 144));
    assert!(super::pack_stop_after(0, 144, 8000, 144));
    assert!(super::pack_stop_after(8001, 1, 8000, 144));
    assert!(!super::pack_stop_after(8000, 1, 8000, 144));
    assert_eq!(
        super::confirm_batch_max_inputs(),
        super::CONFIRM_BATCH_INPUTS_DEFAULT
    );
    assert_eq!(super::write_drain_max_parts(20), 20);
    assert_eq!(super::write_drain_max_parts(4), 4);
    assert_eq!(super::write_drain_max_parts(3), 3);
    assert_eq!(super::write_drain_max_parts(0), 1);
}
