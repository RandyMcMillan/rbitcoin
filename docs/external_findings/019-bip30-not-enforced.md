# BIP30 is not enforced

**Component:** consensus connect (no BIP30 check)
**Audit pin:** fuzzamoto report 008 / rbitcoin `8f3990f`
**Severity:** critical — overwrite live unspent (CVE-2012-1909 class)
**Status:** fixed — BIP34-gated TipOnly batch + spentness on connected sibling
**Found by:** fuzzamoto

## Summary

A new block may contain a txid already on the best chain with **unspent**
outputs. Without BIP30 the new instance overwrites the old in indexes.

Fully-spent earlier instances may be duplicated. Mainnet also **grandfathers**
two overwrites of **unspent** coinbases (Core `IsBIP30Repeat`): **91842**
(`d5d27987…` from 91812, still immature) and **91880** (`e3bf3d07…` from
91722). Those UTXOs were overwritten, not spent — do not `bad-txns-BIP30`
them. A write-batch reject may log the **first** height in the batch (IBD
`@91859` was 91880 inside the same ScriptOkBatch).

Core skips BIP30 once **BIP34** is active (after height 227931 on mainnet),
except the two hashes above which skip even before BIP34.

## Fix

Gate: if `bip34_active_at(height)` skip (hot path). Else if
`is_bip30_repeat(height, hash)` (91842 / 91880 mainnet hashes) skip. Else
TipOnly connected sibling + durable spentness. Never treat just-archived self
as a conflict.
