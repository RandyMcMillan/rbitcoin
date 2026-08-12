# Regtest CLTV / strict DER activation heights may be stale

**Component:** `ChainParams::regtest` / rust-bitcoin inherited heights
**Audit pin:** fuzzamoto report 010 / rbitcoin `8f3990f`
**Severity:** low — regtest only
**Status:** fixed — `ChainParams::regtest` sets bip65/bip66 = 1 (BIP34 left huge)
**Found by:** fuzzamoto

## Summary

Core regtest sets BIP65 and BIP66 to height **1**. rust-bitcoin historically
used 1351 / 1251, so CLTV is a no-op and DER is loose for the whole useful test
range. Signet in-tree already asserts height 1; regtest must be checked.

BIP34 on regtest (Core 1 vs huge rust-bitcoin value) is a **lead**, not required
for this finding.

## Fix

If still stale, set `bip65_height = 1`, `bip66_height = 1` on `ChainParams::regtest`.
Add a low-height CLTV-unsatisfied reject test if missing.
