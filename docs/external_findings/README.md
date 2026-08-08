# External findings (fuzzamoto / differential)

Consensus and P2P issues reported against rbitcoin (Bitcoin Core primary vs
rbitcoin reference). Numbered reports live beside this index.

| ID | Severity | Topic |
|----|----------|--------|
| [001](./001-disconnect-on-invalid-block.md) | medium | Peer disconnect on invalid relayed block (BIP-152) |
| [002](./002-store-corrupt-record-on-invalid-block.md) | low | Invalid block misclassified as store corrupt |
| [003](./003-bip68-version-signedness-consensus-split.md) | high | BIP68 skipped for version with bit 31 set |
| [004](./004-csv-nop-and-scriptnum-width.md) | high | CSV v1 no-op; CLTV/CSV 4-byte scriptnum |
| [005](./005-non-topological-block-accepted.md) | high | Non-topological same-block spends accepted |

Remediation (2026-08): 001–005 fixed in-tree; see each file **Status** line.
