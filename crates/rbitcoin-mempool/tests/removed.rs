use bitcoin::absolute::LockTime;
use bitcoin::hashes::Hash;
use bitcoin::transaction::Version;
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Txid, Witness};
use rbitcoin_mempool::accept::{MapUtxoProvider, PrepareOpts, SiblingEviction};
use rbitcoin_mempool::{
    ActiveMempool, ChainTipCtx, Coin, PoolSequence, Removal, RemovalReason, RemovedRow,
    REMOVED_ROWS,
};
use std::collections::{BTreeSet, HashMap};
use std::fs;

const TIP_OK: ChainTipCtx = ChainTipCtx {
    height: 1_000_000,
    mtp: u32::MAX,
};

fn tmp_dir() -> rbitcoin_store::testutil::TempDir {
    rbitcoin_store::testutil::TempDir::labeled("mempool-removed").unwrap()
}

fn chain_coins(n: u8) -> (Vec<OutPoint>, MapUtxoProvider) {
    let mut map = HashMap::new();
    let mut ops = Vec::new();
    for i in 0..n {
        let op = OutPoint {
            txid: Txid::from_byte_array([i.wrapping_add(1); 32]),
            vout: 0,
        };
        let coin = Coin {
            txout: TxOut {
                value: Amount::from_sat(100_000),
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            },
            create_height: 1,
            create_mtp: 0,
            is_coinbase: false,
            create_fk: None,
        };
        map.insert(op, coin);
        ops.push(op);
    }
    (ops, MapUtxoProvider { map })
}

fn spend(op: OutPoint, out_value: u64) -> Transaction {
    Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: op,
            script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(out_value),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    }
}

/// Core's `mempool_sequence` counts changes. A compact rebuilds the graph and
/// a reopen loads records; neither is a change.
#[test]
fn a_compact_or_a_reopen_is_not_a_change() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(2);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let a = spend(ops[0], 99_000);
    let b = spend(ops[1], 99_000);
    mp.accept_tx(&a, &utxos, TIP_OK).expect("a");
    mp.accept_tx(&b, &utxos, TIP_OK).expect("b");
    mp.accept_tx(&spend(ops[0], 90_000), &utxos, TIP_OK)
        .expect("replacement");
    mp.remove_live_txids(&[b.compute_txid()], BLOCK).unwrap();
    let seq = mp.mempool_sequence();
    let rows = mp.removed_rows();
    mp.compact().unwrap();
    assert_eq!(mp.mempool_sequence(), seq, "a rebuild is not a change");
    assert_eq!(mp.removed_rows(), rows, "a rebuild writes no row");
    mp.flush().unwrap();
    drop(mp);

    let mp = ActiveMempool::open_or_create(&dir).unwrap();
    assert_eq!(mp.live_count(), 1);
    assert_eq!(mp.mempool_sequence(), 1, "loaded records are not changes");
    assert_eq!(mp.removed_rows().oldest, 1);
    assert!(mp.removed_rows().rows.is_empty());
}

const BLOCK: Removal = Removal::Committed(RemovalReason::Block);

/// A compact that cannot install its packed images has to leave the slot
/// table and the graph as they were. A swap without the install leaves the
/// graph naming old slots, so a later removal misses the tx's compacted
/// slot, and the next successful compact reloads the tx from it.
#[test]
fn a_failed_compact_does_not_bring_a_later_removal_back() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(4);
    let mut mp = ActiveMempool::open_or_create(dir.path()).unwrap();
    let txs: Vec<_> = ops.iter().map(|op| spend(*op, 99_000)).collect();
    for tx in &txs {
        mp.accept_tx(tx, &utxos, TIP_OK).expect("admit");
    }
    mp.flush().unwrap();

    // A directory at the body tmp path makes the first image write fail
    // before either rename, which is the read-only-directory failure.
    let blocker = dir.path().join("tx.body.tmp");
    fs::create_dir(&blocker).unwrap();
    let stripped = mp
        .remove_live_txids(&[txs[0].compute_txid(), txs[1].compute_txid()], BLOCK)
        .unwrap();
    assert_eq!(stripped, 2);
    assert!(mp.get_tx(&txs[2].compute_txid()).is_some());
    assert!(mp.get_tx(&txs[3].compute_txid()).is_some());

    // The strip's swallowed `maybe_compact` has already failed. A direct call
    // still errors with the blocker in place, so the test cannot pass by
    // skipping the failed install entirely.
    assert!(mp.compact().is_err());

    // D still sits in its original slot, so this removal marks it DEAD.
    mp.remove_live_txids(&[txs[3].compute_txid()], BLOCK)
        .unwrap();
    assert!(mp.get_tx(&txs[3].compute_txid()).is_none());

    fs::remove_dir(&blocker).unwrap();
    mp.compact().unwrap();
    assert!(
        mp.get_tx(&txs[3].compute_txid()).is_none(),
        "a DEAD slot in the old table became LIVE in the packed one"
    );
    mp.flush().unwrap();
    drop(mp);

    let mp = ActiveMempool::open_or_create(dir.path()).unwrap();
    assert_eq!(mp.live_count(), 1);
    assert!(mp.get_tx(&txs[2].compute_txid()).is_some());
    assert!(mp.get_tx(&txs[3].compute_txid()).is_none());
}

/// A compact whose body rename lands but whose slots rename fails has already
/// committed the packed body. The session must follow that body: later slot
/// writes go to `slots.tmp`, and the next open links it. Keeping the old slot
/// file would revert every removal made after the failure.
#[test]
fn a_failed_slots_rename_keeps_later_removals() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(4);
    let mut mp = ActiveMempool::open_or_create(dir.path()).unwrap();
    let txs: Vec<_> = ops.iter().map(|op| spend(*op, 99_000)).collect();
    for tx in &txs {
        mp.accept_tx(tx, &utxos, TIP_OK).expect("admit");
    }
    mp.flush().unwrap();

    // A directory at `slots` makes the second rename fail with EISDIR after
    // the body rename has already replaced `tx.body`.
    let slots = dir.path().join("slots");
    fs::remove_file(&slots).unwrap();
    fs::create_dir(&slots).unwrap();
    assert!(mp.compact().is_err());

    mp.remove_live_txids(&[txs[3].compute_txid()], BLOCK)
        .unwrap();
    assert!(mp.get_tx(&txs[3].compute_txid()).is_none());

    fs::remove_dir(&slots).unwrap();
    drop(mp);

    let mp = ActiveMempool::open_or_create(dir.path()).unwrap();
    assert_eq!(mp.live_count(), 3);
    assert!(mp.get_tx(&txs[1].compute_txid()).is_some());
    assert!(mp.get_tx(&txs[3].compute_txid()).is_none());
}

fn out0(tx: &Transaction) -> OutPoint {
    OutPoint {
        txid: tx.compute_txid(),
        vout: 0,
    }
}

fn row(seq: u64, tx: &Transaction, reason: RemovalReason) -> RemovedRow {
    RemovedRow {
        seq,
        txid: tx.compute_txid(),
        reason,
    }
}

/// Rows written since `from`, as a set: one call may unlink in any order.
fn rows_from(mp: &ActiveMempool, from: u64) -> BTreeSet<(u64, Txid, RemovalReason)> {
    mp.removed_rows()
        .rows
        .into_iter()
        .filter(|r| r.seq >= from)
        .map(|r| (r.seq, r.txid, r.reason))
        .collect()
}

fn two_rows(
    from: u64,
    a: &Transaction,
    b: &Transaction,
    reason: RemovalReason,
) -> [BTreeSet<(u64, Txid, RemovalReason)>; 2] {
    let (a, b) = (a.compute_txid(), b.compute_txid());
    [
        BTreeSet::from([(from, a, reason), (from + 1, b, reason)]),
        BTreeSet::from([(from, b, reason), (from + 1, a, reason)]),
    ]
}

/// A replacement takes the conflict's descendant out with the same `by`.
#[test]
fn a_replaced_conflict_and_its_descendant_name_the_replacement() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();

    let parent = spend(ops[0], 99_000);
    let child = spend(out0(&parent), 98_000);
    mp.accept_tx(&parent, &utxos, TIP_OK).expect("parent");
    mp.accept_tx(&child, &utxos, TIP_OK).expect("child");
    let at = mp.mempool_sequence();
    let bump = spend(ops[0], 90_000);
    mp.accept_tx(&bump, &utxos, TIP_OK).expect("replacement");
    let by = RemovalReason::Replaced {
        by: bump.compute_txid(),
    };
    assert!(
        two_rows(at, &parent, &child, by).contains(&rows_from(&mp, at)),
        "the conflict and its descendant, replaced by the admit: {:?}",
        mp.removed_rows()
    );
}

/// A full pool of durable chunks pays one death flush per budget pass, and
/// that flush publishes the final count before the pass returns.
#[test]
fn a_budget_pass_syncs_its_deaths_once() {
    use rbitcoin_mempool::store::{write_meta_bytes, META_LEN};

    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(4);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    for (i, op) in ops.iter().enumerate() {
        let tx = spend(*op, 99_000 - 100 * i as u64);
        mp.accept_tx(&tx, &utxos, TIP_OK).expect("admit");
    }
    mp.store.flush().unwrap();
    let meta = dir.path().join("meta");

    mp.max_weight = 0;
    #[cfg(all(unix, debug_assertions))]
    rbitcoin_store::file::arm_fsync_dir_trace();
    assert_eq!(mp.evict_to_budget(None).unwrap().len(), 4, "four chunks");
    // One mark publish and one clear each sync guard/. Four evictions that
    // each flushed would sync it eight times.
    #[cfg(all(unix, debug_assertions))]
    {
        let guard = dir.path().join("guard");
        let syncs = rbitcoin_store::file::take_fsynced_dirs()
            .into_iter()
            .filter(|d| *d == guard)
            .count();
        assert_eq!(syncs, 2, "one guard/flush publish and one clear per pass");
    }

    let raw = std::fs::read(&meta).unwrap();
    let mut got = [0u8; META_LEN];
    got.copy_from_slice(&raw[..META_LEN]);
    let mut want = [0u8; META_LEN];
    let m = mp.store.meta();
    write_meta_bytes(&mut want, m.generation, m.slot_cap, 0);
    assert_eq!(got, want, "the flush publishes the final count");
    assert!(!dir.path().join("guard/flush").exists());
}

#[test]
fn a_replacement_that_does_not_commit_leaves_no_replaced_row() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let victim = spend(ops[0], 99_000);
    let id = mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim").txid;
    let at = mp.mempool_sequence();
    mp.store.fail_next_append = true;
    assert!(mp
        .accept_tx(&spend(ops[0], 90_000), &utxos, TIP_OK)
        .is_err());
    assert!(mp.graph.contains(&id), "the victim is restored");
    assert!(mp.removed_rows().rows.is_empty(), "{:?}", mp.removed_rows());
    assert_eq!(mp.mempool_sequence(), at + 2, "out and back in");
}

#[test]
fn an_undo_writes_no_row_but_moves_the_sequence() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(2);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let victim = spend(ops[0], 99_000);
    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim");
    let bump = mp
        .accept_tx(&spend(ops[0], 90_000), &utxos, TIP_OK)
        .expect("replacement");
    assert_eq!(mp.removed_rows().rows.len(), 1);
    let at = mp.mempool_sequence();
    mp.rollback_accepted_package(std::slice::from_ref(&bump), &utxos, TIP_OK);
    assert!(mp.graph.contains(&victim.compute_txid()));
    assert!(!mp.graph.contains(&bump.txid));
    assert!(
        mp.removed_rows().rows.is_empty(),
        "a rolled-back admit replaced nothing: {:?}",
        mp.removed_rows()
    );
    assert_eq!(mp.mempool_sequence(), at + 2, "out, then the victim back");

    // A listed transaction that left for good keeps that row when a later
    // staged copy of it is taken back. The listed one left for that reason.
    let staged = spend(ops[1], 99_000);
    mp.accept_tx(&staged, &utxos, TIP_OK).expect("listed");
    mp.remove_live_txids(&[staged.compute_txid()], BLOCK)
        .unwrap();
    let kept = mp.removed_rows();
    assert_eq!(kept.rows.len(), 1);
    mp.accept_tx(&staged, &utxos, TIP_OK).expect("staged copy");
    mp.remove_live_txids(&[staged.compute_txid()], Removal::Undo)
        .unwrap();
    assert_eq!(mp.removed_rows(), kept);
}

#[test]
fn the_ring_drops_its_oldest_row_past_the_cap() {
    let mut seq = PoolSequence::default();
    let txid = |i: usize| Txid::from_byte_array([(i % 251) as u8; 32]);
    for i in 0..REMOVED_ROWS {
        seq.note_removed(txid(i), 0, BLOCK);
    }
    let full = seq.removed_rows();
    assert_eq!((full.oldest, full.rows.len()), (1, REMOVED_ROWS));
    seq.note_removed(txid(REMOVED_ROWS), 0, BLOCK);
    seq.note_removed(txid(REMOVED_ROWS + 1), 0, BLOCK);
    let past = seq.removed_rows();
    assert_eq!(past.rows.len(), REMOVED_ROWS);
    assert_eq!(past.rows[0].seq, 3, "rows 1 and 2 dropped");
    assert_eq!(past.oldest, 3);
    assert_eq!(seq.next(), REMOVED_ROWS as u64 + 3);
}

/// A rollback drops the row of the stay it takes back, and no other. The
/// same txid admitted again and mined keeps its `block` row.
#[test]
fn a_rollback_keeps_the_row_of_a_later_stay() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let tx = spend(ops[0], 99_000);
    let id = tx.compute_txid();
    let staged = mp.accept_tx(&tx, &utxos, TIP_OK).expect("staged");
    mp.remove_live_txids(&[id], Removal::Committed(RemovalReason::SizeLimit))
        .unwrap();
    mp.accept_tx(&tx, &utxos, TIP_OK).expect("admitted again");
    mp.remove_live_txids(&[id], BLOCK).unwrap();
    let mined = *mp.removed_rows().rows.last().unwrap();
    assert_eq!(mined.reason, RemovalReason::Block);

    mp.forget_admits(std::slice::from_ref(&staged));
    assert_eq!(mp.removed_rows().rows, vec![mined]);
}

/// A compact rebuilds the graph and keeps each entry's stay, so a rollback
/// after it still finds the row of the admit it takes back.
#[test]
fn a_rollback_finds_its_row_after_a_compact() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let tx = spend(ops[0], 99_000);
    let staged = mp.accept_tx(&tx, &utxos, TIP_OK).expect("staged");
    mp.compact().unwrap();
    mp.remove_live_txids(&[staged.txid], Removal::Committed(RemovalReason::SizeLimit))
        .unwrap();
    assert_eq!(mp.removed_rows().rows.len(), 1);
    mp.forget_admits(std::slice::from_ref(&staged));
    assert!(mp.removed_rows().rows.is_empty(), "{:?}", mp.removed_rows());
}

/// A rollback drops the `replaced` row its own admit wrote. The same pair
/// replacing again later keeps its row.
#[test]
fn a_rollback_keeps_a_later_replacement_by_the_same_txid() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let victim = spend(ops[0], 99_000);
    let bump = spend(ops[0], 90_000);
    let by = bump.compute_txid();
    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim");
    let staged = mp.accept_tx(&bump, &utxos, TIP_OK).expect("staged bump");
    mp.remove_live_txids(&[by], Removal::Committed(RemovalReason::SizeLimit))
        .unwrap();
    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim again");
    mp.accept_tx(&bump, &utxos, TIP_OK).expect("bump again");
    mp.remove_live_txids(&[by], BLOCK).unwrap();
    let rows = mp.removed_rows().rows;
    assert_eq!(rows.len(), 4, "{rows:?}");
    let later = vec![rows[2], rows[3]];
    assert_eq!(later[0].reason, RemovalReason::Replaced { by });
    assert_eq!(later[1].reason, RemovalReason::Block);

    // The first bump's own eviction goes. Its victim's row waits for the
    // victim to be back.
    mp.forget_admits(std::slice::from_ref(&staged));
    assert_eq!(mp.removed_rows().rows, vec![rows[0], rows[2], rows[3]]);
    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim back");
    assert_eq!(mp.removed_rows().rows, later);
}

/// Two rollbacks that overlap leave an earlier, committed replacement of
/// the same pair alone. The second finds its own row already gone and
/// takes no other in its place.
#[test]
fn overlapping_rollbacks_keep_an_earlier_replacement() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let victim = spend(ops[0], 99_000);
    let bump = spend(ops[0], 90_000);
    let by = bump.compute_txid();
    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim");
    mp.accept_tx(&bump, &utxos, TIP_OK).expect("bump replaces");
    mp.remove_live_txids(&[by], Removal::Committed(RemovalReason::SizeLimit))
        .unwrap();
    let committed = mp.removed_rows().rows;
    assert_eq!(committed.len(), 2, "{committed:?}");

    let staged_victim = mp
        .accept_tx(&victim, &utxos, TIP_OK)
        .expect("staged victim");
    let staged_bump = mp.accept_tx(&bump, &utxos, TIP_OK).expect("staged bump");
    assert_eq!(mp.removed_rows().rows.len(), 3);
    // The staged victim did leave for the bump, which is still live.
    mp.forget_admits(std::slice::from_ref(&staged_victim));
    assert_eq!(mp.removed_rows().rows.len(), 3);
    // The bump goes and its victim is put back, so that row goes too.
    mp.rollback_accepted_package(std::slice::from_ref(&staged_bump), &utxos, TIP_OK);
    assert!(mp.graph.contains(&victim.compute_txid()));
    assert_eq!(mp.removed_rows().rows, committed);
}

/// A member that a block, a replacement, an expiry or a conflict took out
/// before its package rolled back really left for that reason. The rollback
/// keeps that row. It drops only the member's own size-limit eviction.
#[test]
fn a_rollback_keeps_a_real_removal_of_its_member() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let tx = spend(ops[0], 99_000);
    let staged = mp.accept_tx(&tx, &utxos, TIP_OK).expect("staged");
    mp.remove_live_txids(&[staged.txid], BLOCK).unwrap();
    let mined = mp.removed_rows().rows;
    assert_eq!(mined.len(), 1);
    mp.forget_admits(std::slice::from_ref(&staged));
    assert_eq!(mp.removed_rows().rows, mined);
}

/// A replacement that does not commit leaves its victim's row until the
/// victim is back in the pool. A victim that cannot be put back is gone,
/// and its row says what took it out.
#[test]
fn a_failed_replacement_keeps_its_row_until_the_victim_is_back() {
    let dir = tmp_dir();
    let (ops, utxos) = chain_coins(1);
    let mut mp = ActiveMempool::open_or_create(&dir).unwrap();
    let victim = spend(ops[0], 99_000);
    let id = mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim").txid;
    let bump = spend(ops[0], 90_000);
    let prep = mp
        .prepare_admit(
            &bump,
            &utxos,
            TIP_OK,
            PrepareOpts {
                fee_delta: 0,
                report_orphans: false,
                min_relay: Some(0),
                waive: false,
                sibling_eviction: SiblingEviction::Off,
                tested: None,
            },
        )
        .expect("bump prepares");
    mp.store.fail_next_append = true;
    assert!(mp
        .commit_after_script(std::sync::Arc::new(bump.clone()), prep)
        .is_err());
    let out = vec![row(
        2,
        &victim,
        RemovalReason::Replaced {
            by: bump.compute_txid(),
        },
    )];
    assert_eq!(mp.removed_rows().rows, out, "the victim is out");

    let no_coins = MapUtxoProvider {
        map: HashMap::new(),
    };
    mp.restore_undone(&no_coins, TIP_OK);
    assert!(!mp.graph.contains(&id));
    assert_eq!(mp.removed_rows().rows, out, "it could not be put back");

    mp.accept_tx(&victim, &utxos, TIP_OK).expect("victim back");
    assert!(mp.removed_rows().rows.is_empty());
}
