# BIP30 is not enforced

**Component:** consensus connect (no BIP30 check)
**Audit pin:** fuzzamoto report 008 / rbitcoin `8f3990f`
**Severity:** critical — overwrite live unspent (CVE-2012-1909 class)
**Status:** fixed — BIP34-gated TipOnly batch + spentness on connected sibling
**Found by:** fuzzamoto

## Summary

A new block may contain a txid already on the best chain with **unspent**
outputs. Without BIP30 the new instance overwrites the old in indexes. Mainnet
allows **fully spent** duplicates at 91722 / 91880 — do not reject those.

Core skips BIP30 once **BIP34** is active (after height 227931 on mainnet).

## Fix

Gate: if `bip34_active_at(height)` skip (hot path). Else union create txids into
confirm’s existing head batch; spentness **only** if a **connected sibling**
exists. Never treat just-archived self (unconnected) as a conflict.
