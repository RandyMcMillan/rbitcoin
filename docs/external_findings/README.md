# External findings (fuzzamoto / differential / redteam)

Consensus, P2P, and mempool issues reported against rbitcoin (Bitcoin Core primary vs
rbitcoin reference, or redteam static analysis). Numbered reports live beside this index.

| ID | Severity | Topic | Status | Regression (shipped) |
|----|----------|--------|--------|----------------------|
| [001](./001-disconnect-on-invalid-block.md) | medium | Peer disconnect on invalid relayed block (BIP-152) | fixed | `rbitcoin-net` `peer::tests::cmpct_helpers_without_mempool_and_queue_out_closed` |
| [002](./002-store-corrupt-record-on-invalid-block.md) | low | Invalid block misclassified as store corrupt | fixed | `rbitcoin-consensus` `error::tests::archive_unresolved_parent_is_missing_prevout_not_corrupt` |
| [003](./003-bip68-version-signedness-consensus-split.md) | high | BIP68 skipped for version with bit 31 set | fixed | `block::tests::bip68_enforced_when_version_high_bit_set` |
| [004](./004-csv-nop-and-scriptnum-width.md) | high | CSV v1 no-op; CLTV/CSV 4-byte scriptnum | fixed | `script::interpreter::tests::csv_fails_when_tx_version_below_2` + Core script corpus |
| [005](./005-non-topological-block-accepted.md) | high | Non-topological same-block spends accepted | fixed | `rbitcoin-test` `consensus_rules::c8_same_block_child_before_parent_rejected` |
| [006](./006-p2sh-scriptsig-push-size.md) | medium | P2SH scriptSig pushes not limited to 520 bytes | fixed | `script::nested::tests::p2sh_scriptsig_push_over_520_rejected` |
| [007](./007-p2sh-nested-witness-exactness.md) | medium | P2SH nested-witness scriptSig exactness / program rules | fixed | `script::nested` nested-witness malleation tests |
| [008](./008-p2tr-keypath-sighash-zero.md) | medium | P2TR key-path 65-byte sig with sighash byte 0x00 | fixed | `script::p2tr::tests::key_path_rejects_65_byte_sighash_byte_zero` |
| [009](./009-witness-commitment-reserved.md) | medium | Witness commitment empty/multi-item coinbase witness | fixed | `block::tests::s8_rejects_empty_or_multi_item_coinbase_witness_reserved` |
| [010](./010-mempool-confirmed-spentness.md) | medium | Mempool no confirmed-chain spentness check | fixed | `rbitcoin-mempool` `accept::tests::reject_when_provider_has_no_unspent_coin` |
| [011](./011-mempool-structural-chain-context.md) | medium | Mempool no structural chain-context validation | fixed | `accept::tests::reject_non_final_locktime_height`, `reject_immature_coinbase` |

**Policy:** Core `script_tests` / `tx_valid` / `tx_invalid` corpora must pass **every**
data row with **no allowlist**. Do not commit if those tests fail. Findings stay
**fixed** with a named regression on the shipped path.

**006–009:** consensus accept-invalid (zip 2026-08-10) — **fixed** in-tree. **010–011:**
mempool remediation **fixed** (Coin spentness + structural tip checks + fee path).

Remediation (2026-08): 001–005 fixed in-tree; see each file **Status** and **Regression**.
