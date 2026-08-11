# External findings (fuzzamoto / differential / redteam)

Consensus, P2P, and mempool issues reported against rbitcoin (Bitcoin Core primary vs
rbitcoin reference, or redteam static analysis). Numbered reports live beside this index.

| ID | Severity | Topic | Status |
|----|----------|--------|--------|
| [001](./001-disconnect-on-invalid-block.md) | medium | Peer disconnect on invalid relayed block (BIP-152) | fixed |
| [002](./002-store-corrupt-record-on-invalid-block.md) | low | Invalid block misclassified as store corrupt | fixed |
| [003](./003-bip68-version-signedness-consensus-split.md) | high | BIP68 skipped for version with bit 31 set | fixed |
| [004](./004-csv-nop-and-scriptnum-width.md) | high | CSV v1 no-op; CLTV/CSV 4-byte scriptnum | fixed |
| [005](./005-non-topological-block-accepted.md) | high | Non-topological same-block spends accepted | fixed |
| [006](./006-p2sh-scriptsig-push-size.md) | medium | P2SH scriptSig pushes not limited to 520 bytes | fixed |
| [007](./007-p2sh-nested-witness-exactness.md) | medium | P2SH nested-witness scriptSig exactness / program rules | fixed |
| [008](./008-p2tr-keypath-sighash-zero.md) | medium | P2TR key-path 65-byte sig with sighash byte 0x00 | fixed |
| [009](./009-witness-commitment-reserved.md) | medium | Witness commitment empty/multi-item coinbase witness | fixed |
| [010](./010-mempool-confirmed-spentness.md) | medium | Mempool no confirmed-chain spentness check | fixed |
| [011](./011-mempool-structural-chain-context.md) | medium | Mempool no structural chain-context validation | fixed |

**006–009:** consensus accept-invalid (zip 2026-08-10) — **fixed** in-tree. **010–011:** mempool/0-conf;
remediation **fixed** (Coin spentness + structural tip checks + 10-minute inclusion fee).

Remediation (2026-08): 001–005 fixed in-tree; see each file **Status** line.
