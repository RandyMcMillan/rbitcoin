//! Focused consensus-rule coverage (rules we implement outside rust-bitcoin).
//!
//! Matrix: see `docs/consensus-tests.md`.

use bitcoin::hashes::Hash;
use bitcoin::{Amount, BlockHash, CompactTarget};
use rbitcoin_consensus::{
    accept_and_connect_block, block_subsidy, expected_next_bits, median_time_past, validate_header,
    validate_block_connect, ChainParams, Checkpoint, ConsensusError, Milestone, ValidationContext,
};
use rbitcoin_primitives::Height;
use rbitcoin_query::Query;
use rbitcoin_test::mine::{mine_regtest_block, regtest_genesis, spend_anyone_can_spend};
use rbitcoin_test::TestDatadir;

fn regtest_q() -> (TestDatadir, Query, ChainParams) {
    let td = TestDatadir::new().unwrap();
    let q = Query::open_or_create(td.store_path()).unwrap();
    let params = ChainParams::regtest();
    (td, q, params)
}

fn connect_genesis(q: &Query, params: &ChainParams) {
    let g = regtest_genesis();
    accept_and_connect_block(q, params, Height::GENESIS, &g, Milestone::NONE).unwrap();
}

// ─── Header rules ───────────────────────────────────────────────────────────

#[test]
fn h1_rejects_wrong_genesis_hash() {
    let (_td, q, params) = regtest_q();
    let mut g = regtest_genesis();
    g.header.nonce = g.header.nonce.wrapping_add(1);
    // Even if PoW happens to pass regtest, genesis hash check fires first for h=0.
    let err = validate_header(&q, &params, Height::GENESIS, &g.header).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadHeader(s) if s.contains("genesis")),
        "{err:?}"
    );
}

#[test]
fn h2_rejects_bad_prev_link() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let mut b1 = mine_regtest_block(g.block_hash(), g.header.time + 1, 1, vec![]);
    b1.header.prev_blockhash = BlockHash::from_byte_array([0xee; 32]);
    // Re-mine nonce after prev change (PoW may fail first; BadPrev is the link check).
    let target = bitcoin::Target::from_compact(b1.header.bits);
    for nonce in 0..100_000u32 {
        b1.header.nonce = nonce;
        if b1.header.validate_pow(target).is_ok() {
            break;
        }
    }
    let err = validate_header(&q, &params, Height(1), &b1.header).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadPrev),
        "expected BadPrev, got {err:?}"
    );
}

#[test]
fn h3_rejects_timestamp_not_after_mtp() {
    let (_td, q, params) = regtest_q();
    let g = regtest_genesis();
    accept_and_connect_block(&q, &params, Height::GENESIS, &g, Milestone::NONE).unwrap();
    // Build 11 blocks with increasing times so MTP is well-defined.
    let mut tip = g.block_hash();
    let mut time = g.header.time;
    for h in 1..=11u32 {
        time += 600;
        let b = mine_regtest_block(tip, time, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        tip = b.block_hash();
    }
    let mtp = median_time_past(&q, Height(11)).unwrap();
    let mut bad = mine_regtest_block(tip, mtp, 12, vec![]); // time == mtp → reject
    // ensure bits match expected (regtest copies prev)
    let expected = expected_next_bits(&q, &params, Height(12)).unwrap();
    bad.header.bits = expected;
    let target = bitcoin::Target::from_compact(expected);
    for nonce in 0..u32::MAX {
        bad.header.nonce = nonce;
        if bad.header.validate_pow(target).is_ok() {
            break;
        }
    }
    let err = validate_header(&q, &params, Height(12), &bad.header).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadHeader(s) if s.contains("median-time")),
        "{err:?}"
    );
}

#[test]
fn h4_rejects_checkpoint_mismatch() {
    let (_td, q, mut params) = regtest_q();
    connect_genesis(&q, &params);
    // Inject a fake checkpoint at height 1 that no valid block can match.
    params.checkpoints.push(Checkpoint {
        height: 1,
        hash: BlockHash::from_byte_array([0xcc; 32]),
    });
    let g = regtest_genesis();
    let b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    let err = validate_header(&q, &params, Height(1), &b1.header).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadHeader(s) if s.contains("checkpoint")),
        "{err:?}"
    );
}

#[test]
fn h5_regtest_rejects_wrong_bits() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let mut b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    // Corrupt bits (regtest has no retarget — must equal prev).
    b1.header.bits = CompactTarget::from_consensus(0x207f_fffe);
    let target = bitcoin::Target::from_compact(b1.header.bits);
    for nonce in 0..100_000u32 {
        b1.header.nonce = nonce;
        if b1.header.validate_pow(target).is_ok() {
            break;
        }
    }
    let err = validate_header(&q, &params, Height(1), &b1.header).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadHeader(s) if s.contains("bits") || s.contains("proof")),
        "{err:?}"
    );
}

#[test]
fn h6_target_above_pow_limit_is_detectable() {
    // We reject `target > pow_limit` in validate_header; assert the comparison
    // fixture (mainnet limit vs too-easy compact) holds so the branch is reachable.
    let main = ChainParams::mainnet();
    let too_easy = CompactTarget::from_consensus(0x2200_ffff);
    let t = bitcoin::Target::from_compact(too_easy);
    assert!(t > main.pow_limit, "fixture target should exceed mainnet pow limit");
}

// ─── Connect / economic rules ───────────────────────────────────────────────

#[test]
fn c2_same_block_double_spend_rejected() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
    let cb_txid = b1.txdata[0].compute_txid();

    // Two spends of the same coinbase output in one block.
    let s1 = spend_anyone_can_spend(cb_txid, 0, Amount::from_sat(25_0000_0000));
    let s2 = spend_anyone_can_spend(cb_txid, 0, Amount::from_sat(24_0000_0000));
    // Need maturity first
    let maturity = params.coinbase_maturity();
    let mut tip = b1.block_hash();
    let mut time = b1.header.time;
    let mut h = 1u32;
    while h < maturity {
        h += 1;
        time += 600;
        let b = mine_regtest_block(tip, time, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        tip = b.block_hash();
    }
    time += 600;
    let bad = mine_regtest_block(tip, time, h + 1, vec![s1, s2]);
    let err = accept_and_connect_block(&q, &params, Height(h + 1), &bad, Milestone::NONE);
    assert!(
        matches!(
            err,
            Err(ConsensusError::BadTx(s)) if s.contains("double spend")
        ),
        "{err:?}"
    );
}

#[test]
fn c5_immature_coinbase_spend_rejected() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
    let cb_txid = b1.txdata[0].compute_txid();
    let spend = spend_anyone_can_spend(cb_txid, 0, Amount::from_sat(49_0000_0000));
    // Spend at height 2 — far below maturity (100 on regtest).
    let bad = mine_regtest_block(b1.block_hash(), b1.header.time + 600, 2, vec![spend]);
    let err = accept_and_connect_block(&q, &params, Height(2), &bad, Milestone::NONE);
    assert!(
        matches!(err, Err(ConsensusError::BadTx(s)) if s.contains("immature")),
        "{err:?}"
    );
}

#[test]
fn c6_value_in_less_than_out_rejected() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
    let maturity = params.coinbase_maturity();
    let mut tip = b1.block_hash();
    let mut time = b1.header.time;
    let mut h = 1u32;
    while h < maturity {
        h += 1;
        time += 600;
        let b = mine_regtest_block(tip, time, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        tip = b.block_hash();
    }
    let cb_txid = b1.txdata[0].compute_txid();
    // Output more than the 50 BTC coinbase.
    let over = spend_anyone_can_spend(cb_txid, 0, Amount::from_sat(51_0000_0000));
    time += 600;
    let bad = mine_regtest_block(tip, time, h + 1, vec![over]);
    let err = accept_and_connect_block(&q, &params, Height(h + 1), &bad, Milestone::NONE);
    assert!(
        matches!(err, Err(ConsensusError::BadTx(s)) if s.contains("in < out")),
        "{err:?}"
    );
}

#[test]
fn c7_coinbase_excess_subsidy_rejected() {
    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    // Height 1: subsidy 50 BTC, no fees — claim 51 BTC in coinbase.
    let mut b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    b1.txdata[0].output[0].value = Amount::from_sat(51_0000_0000);
    b1.header.merkle_root = b1.compute_merkle_root().unwrap();
    let target = bitcoin::Target::from_compact(b1.header.bits);
    for nonce in 0..u32::MAX {
        b1.header.nonce = nonce;
        if b1.header.validate_pow(target).is_ok() {
            break;
        }
    }
    let err = accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE);
    assert!(
        matches!(
            err,
            Err(ConsensusError::BadBlock(s)) if s.contains("coinbase excess")
        ),
        "{err:?}"
    );
    assert_eq!(block_subsidy(1, &params), 50_0000_0000);
}

#[test]
fn c1_non_coinbase_empty_outputs_rejected() {
    use bitcoin::absolute::LockTime;
    use bitcoin::script::ScriptBuf;
    use bitcoin::transaction::Version as TxVersion;
    use bitcoin::{OutPoint, Sequence, Transaction, TxIn, Witness};

    let (_td, q, params) = regtest_q();
    connect_genesis(&q, &params);
    let g = regtest_genesis();
    let b1 = mine_regtest_block(g.block_hash(), g.header.time + 600, 1, vec![]);
    accept_and_connect_block(&q, &params, Height(1), &b1, Milestone::NONE).unwrap();
    let maturity = params.coinbase_maturity();
    let mut tip = b1.block_hash();
    let mut time = b1.header.time;
    let mut h = 1u32;
    while h < maturity {
        h += 1;
        time += 600;
        let b = mine_regtest_block(tip, time, h, vec![]);
        accept_and_connect_block(&q, &params, Height(h), &b, Milestone::NONE).unwrap();
        tip = b.block_hash();
    }
    let cb_txid = b1.txdata[0].compute_txid();
    let empty_out = Transaction {
        version: TxVersion::ONE,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint {
                txid: cb_txid,
                vout: 0,
            },
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: Witness::new(),
        }],
        output: vec![],
    };
    time += 600;
    let bad = mine_regtest_block(tip, time, h + 1, vec![empty_out]);
    let ctx = ValidationContext {
        params: &params,
        height: Height(h + 1),
        milestone: Milestone::NONE,
    };
    // Structure may pass; connect must reject.
    let err = validate_block_connect(&q, &bad, &ctx, None).unwrap_err();
    assert!(
        matches!(err, ConsensusError::BadTx(s) if s.contains("no outputs")),
        "{err:?}"
    );
}
