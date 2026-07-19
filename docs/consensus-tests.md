# Consensus-rule test matrix

Every consensus rule **we implement** (not delegated wholesale to rust-bitcoin) has an automated test that would fail if the check were removed or inverted.

**Out of scope:** full secp256k1 / script-interpreter opcode parity vs Core; rust-bitcoin PoW / `CompactTarget` retarget math; full mainnet retarget golden vectors.

## Running

```bash
nix-shell
cargo test -p rbitcoin-consensus --lib
cargo test -p rbitcoin-test --test consensus_rules
# broader integration still covers connect success paths:
cargo test -p rbitcoin-test --test scenarios consensus_
```

## A. Block structure — `validate_block_structure_hashed`

| ID | Rule | Error signal | Test |
|----|------|--------------|------|
| S1 | Block has ≥1 tx | `BadBlock("no transactions")` | `structure_rule_tests::s1_rejects_empty_txdata` |
| S2 | First tx is coinbase | `BadBlock("first tx not coinbase")` | `structure_rule_tests::s2_rejects_non_coinbase_first` |
| S3 | No later coinbase | `BadBlock("coinbase not first")` | `structure_rule_tests::s3_rejects_second_coinbase` |
| S4 | Weight ≤ 4_000_000 WU | `BadBlock("…weight…")` | `structure_rule_tests::s4_rejects_overweight_block` |
| S5 | Unique txids | `BadBlock("duplicate txid")` | `structure_rule_tests::s5_rejects_duplicate_txid` |
| S6 | Merkle root matches txids | `BadBlock("merkle root mismatch")` | `structure_rule_tests::s6_rejects_merkle_root_mismatch` (+ `merkle_root_bytes_single_and_odd`) |
| S7 | BIP34 height in coinbase (h≥1) | `BadBlock("bip34…")` | `s7_rejects_bip34_missing_at_height_1`, `s7_bip34_not_required_at_height_0` |
| S8 | Witness commitment when any witness | missing / mismatch | `s8_rejects_missing_witness_commitment`, `s8_rejects_wrong_witness_commitment` |

Location: `crates/rbitcoin-consensus/src/block.rs` (`structure_rule_tests`).

## B. Header — `validate_header` / helpers

| ID | Rule | Error signal | Test |
|----|------|--------------|------|
| H1 | Genesis hash matches params | `BadHeader("genesis hash mismatch")` | `h1_rejects_wrong_genesis_hash` |
| H2 | `prev` links to height−1 | `BadPrev` | `h2_rejects_bad_prev_link` |
| H3 | `time > median_time_past` | `timestamp <= median-time-past` | `h3_rejects_timestamp_not_after_mtp` |
| H4 | Checkpoint hash at height | `checkpoint mismatch` | `h4_rejects_checkpoint_mismatch` |
| H5 | `bits == expected_next_bits` | `incorrect proof of work bits` | `h5_regtest_rejects_wrong_bits` (regtest: must equal prev) |
| H6 | Target ≤ `pow_limit` | `target above pow limit` | `h6_target_above_pow_limit_is_detectable` |
| H7 | PoW valid for claimed bits | `InvalidPow` | **rust-bitcoin** `validate_pow` — smoke via any successful `mine_regtest_block` accept |

Location: `crates/rbitcoin-test/tests/consensus_rules.rs`. Helpers: `median_time_past`, `expected_next_bits` exported from `rbitcoin_consensus`.

## C. Connect — `connect_block_prevouts` / `validate_block_connect`

| ID | Rule | Error signal | Test |
|----|------|--------------|------|
| C1 | Non-coinbase has inputs/outputs | `BadTx("no … outputs")` | `c1_non_coinbase_empty_outputs_rejected` |
| C2 | Same-block double-spend | `BadTx("double spend in block")` | `c2_same_block_double_spend_rejected` |
| C3 | Already spent on **best chain** (strong spenders only; archive-ahead point rows ignored) | `PrevoutSpent` | `scenarios::confirm_with_spend_index_ignores_archive_only_point_edges` (signet @2148 class), mature-chain spend scenarios |
| C4 | Missing prevout | `MissingPrevout` / prevout fail | `scenarios::consensus_reject_bad_structure_and_milestone` (milestone still checks prevouts) |
| C5 | Coinbase maturity | `BadTx("coinbase immature")` | `c5_immature_coinbase_spend_rejected` |
| C6 | Value in ≥ out | `BadTx("in < out")` | `c6_value_in_less_than_out_rejected` |
| C7 | Coinbase ≤ subsidy + fees | `BadBlock("coinbase excess value")` | `c7_coinbase_excess_subsidy_rejected` |
| C8 | Overflows on value/fee sums | `… overflow` | optional; not crafted |
| C9 | Scripts via pure-Rust backend | `Script(…)` | `script::tests_verify` (P2WPKH/P2PKH/P2WSH/P2SH-P2WPKH) + `script::p2tr::bip341_tests` (key-path + script-path BIP341 tweak) + mature spend scenarios |

## D. Params / policy we define

| ID | Rule | Test |
|----|------|------|
| P1 | `block_subsidy` halvings | `structure_rule_tests::p1_block_subsidy_halvings` (+ C7 uses height-1 subsidy) |
| P2 | Milestone skips **scripts only** | `scenarios::consensus_reject_bad_structure_and_milestone` |
| P3 | `default_milestone_height` per network | `structure_rule_tests::p3_default_milestone_heights` |
| P4 | Genesis hash check | H1 + accept genesis in scenarios |

## Ownership notes

| Delegated to dependency | Our responsibility still tested |
|-------------------------|----------------------------------|
| `block.weight()`, `compute_txid` / `compute_wtxid` | Thresholds (S4), uniqueness (S5), commitment logic (S8) |
| `Header::validate_pow`, `CompactTarget::from_next_work_required` | Orchestration: bits match expected (H5), target ≤ limit (H6) |
| Pure-Rust script verify (`rbitcoin-consensus::script`) | Connect calls it under `Milestone::NONE`; maturity/value rules are ours. BIP341 commitment via `ControlBlock::verify_taproot_commitment`. BIP342 **OP_SUCCESSx** (incl. 0x7e) succeeds on tapscript; legacy OP_CAT is **disabled** (consensus fail if executed). |
| Policy (`rbitcoin-consensus::policy`) | **Never** on block connect — relay/standardness only (stub). |
| BIP325 signet challenge | `validate_signet_block_solution` on **tip confirm/connect only** (not archive structure — IBD prep cost) |

### Script backend (C9 detail)

| Path | Accept | Reject |
|------|--------|--------|
| P2WPKH | `p2wpkh_valid_signature_accepts` | `p2wpkh_bad_signature_rejects` |
| P2PKH | `p2pkh_valid_signature_accepts` | — |
| P2WSH | `p2wsh_op_true_accepts` | `p2wsh_wrong_script_hash_rejects` |
| P2SH-P2WPKH | `p2sh_p2wpkh_nested_accepts` | — |
| P2SH legacy multi-push (true top, not cleanstack) | `p2sh_legacy_multi_push_op_true_accepts` | — |
| P2TR key-path | `key_path_accepts_valid_schnorr` | `key_path_rejects_bad_sig` |
| P2TR script-path + BIP341 | `script_path_accepts_with_valid_bip341_tweak`, `script_path_two_leaf_merkle_path` | `script_path_rejects_wrong_output_key`, `script_path_rejects_tampered_control_block` |
| Anyone-can-spend | `anyone_can_spend_accepts` | — |
| Core `script_tests.json` (non-sig rows) | `script::core_vectors::core_script_tests_nonsig_majority_pass` | — |

### Signet reject-class fixtures (`tests/fixtures/signet_block_*.bin`)

| Height | Failure class (historical) | Test |
|--------|----------------------------|------|
| 1 | BIP325 signet challenge | `signet::tests::signet_block_1_solution_valid` |
| 200001 | BIP342 CHECKSIGADD `0xba` / OP_SUCCESS | `signet_200001_checksigadd` |
| 200945 | OP_1SUB `0x8c` unary arith | `signet_200945_op1sub` |
| 201393 | Tapscript size > 10k (no legacy limit) | `signet_201393_large_tapscript` |
| 204802 | P2SH multi-push must fall through nested probe | `signet_204802_p2sh` |
| 2148 | Archive point edges ≠ best-chain spent | `scenarios::confirm_with_spend_index_ignores_archive_only_point_edges` |
| 90719 | BIP342 OP_CODESEPARATOR instruction index in tapscript sighash | `signet_90719_codeseparator` + `script_path_codeseparator_checksig_chain` |
| 219477 | BIP16 true-top (not cleanstack) | `signet_219477_p2sh_true_top` + `p2sh_legacy_multi_push_op_true_accepts` |

Benches: `cargo bench -p rbitcoin-consensus --bench script_verify` (real P2WPKH / P2TR key-path).

Dependency gate: `cargo tree -i bitcoinconsensus` must not resolve.

## Adding a new rule

1. Add a row to the inventory above (or mark **rust-bitcoin** / **lib** if delegated).
2. Prefer a pure unit test in `rbitcoin-consensus` when no chain state is needed; otherwise `consensus_rules` or a focused scenario.
3. Name the test after the rule ID when practical (`s4_…`, `h3_…`, `c2_…`).
4. Assert on the **error signal** string/variant so removing the check fails the test.
