//! finality_tests (peeled from block.rs).

use super::{is_final_tx, sequence_locks_satisfied, LOCKTIME_THRESHOLD};
use bitcoin::absolute::LockTime;
use bitcoin::script::ScriptBuf;
use bitcoin::{Amount, OutPoint, Sequence, Transaction, TxIn, TxOut, Witness};

fn bare_tx(version: i32, lock_time: LockTime, sequence: Sequence) -> Transaction {
    Transaction {
        version: bitcoin::transaction::Version(version),
        lock_time,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence,
            witness: Witness::new(),
        }],
        output: vec![TxOut {
            value: Amount::from_sat(1),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }],
    }
}

#[test]
fn final_when_locktime_zero() {
    let tx = bare_tx(1, LockTime::ZERO, Sequence::MAX);
    assert!(is_final_tx(&tx, 100, 1_000_000));
}

#[test]
fn height_locktime_not_final_until_height() {
    let tx = bare_tx(1, LockTime::from_height(100).unwrap(), Sequence::ZERO);
    assert!(!is_final_tx(&tx, 100, 1_000_000)); // need lt < height
    assert!(is_final_tx(&tx, 101, 1_000_000));
}

#[test]
fn sequence_final_ignores_locktime() {
    let tx = bare_tx(1, LockTime::from_height(100).unwrap(), Sequence::MAX);
    assert!(is_final_tx(&tx, 50, 1_000_000));
}

#[test]
fn time_locktime_uses_cutoff() {
    let t = LOCKTIME_THRESHOLD + 1000;
    let tx = bare_tx(1, LockTime::from_time(t).unwrap(), Sequence::ZERO);
    assert!(!is_final_tx(&tx, 1, t)); // need lt < cutoff
    assert!(is_final_tx(&tx, 1, t + 1));
}

#[test]
fn bip68_height_relative_lock() {
    // version 2, seq = 10 (height), coin at height 100 → minHeight = 100+10-1 = 109
    // needs block_height > 109
    let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(10));
    assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
    assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
}

#[test]
fn bip68_disabled_by_version_1() {
    let tx = bare_tx(1, LockTime::ZERO, Sequence::from_consensus(10));
    assert!(sequence_locks_satisfied(&tx, &[100], &[0], 50, 0));
}

/// Core treats nVersion as unsigned: 0xFFFFFFFF ≥ 2 → BIP68 enforced
/// (`docs/external_findings/003-bip68-version-signedness-consensus-split.md`).
#[test]
fn bip68_enforced_when_version_high_bit_set() {
    // rust-bitcoin Version(i32): -1 is wire 0xFFFFFFFF.
    let tx = bare_tx(-1, LockTime::ZERO, Sequence::from_consensus(10));
    assert!(super::bip68_active_for_tx(&tx));
    // Same relative height lock as bip68_height_relative_lock — must fail at h=109.
    assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
    assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
}

#[test]
fn bip68_disable_flag_and_time_type() {
    // DISABLE bit → ignore relative lock.
    let disable = 1u32 << 31;
    let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(disable | 10));
    assert!(sequence_locks_satisfied(&tx, &[100], &[0], 50, 0));

    // Time-based: TYPE_FLAG | n, granularity 512s.
    let type_flag = 1u32 << 22;
    let n = 2u32; // 2 × 512s relative
    let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(type_flag | n));
    // coin MTP = 1000; minTime ≈ 1000 + (2<<9) - 1
    let coin_mtp = 1000u32;
    let min_time = coin_mtp as i64 + ((n as i64) << 9) - 1;
    assert!(!sequence_locks_satisfied(
        &tx,
        &[100],
        &[coin_mtp],
        200,
        (min_time as u32).saturating_sub(1)
    ));
    assert!(sequence_locks_satisfied(
        &tx,
        &[100],
        &[coin_mtp],
        200,
        min_time as u32 + 1
    ));
}

/// Height-type locks ignore coin MTP (write path may leave mtps as 0).
#[test]
fn bip68_height_type_ignores_zero_mtp() {
    let tx = bare_tx(2, LockTime::ZERO, Sequence::from_consensus(10));
    assert!(sequence_locks_satisfied(&tx, &[100], &[0], 110, 0));
    // bogus MTP must not affect height-type check
    assert!(sequence_locks_satisfied(&tx, &[100], &[u32::MAX], 110, 0));
    assert!(!sequence_locks_satisfied(&tx, &[100], &[0], 109, 0));
}
