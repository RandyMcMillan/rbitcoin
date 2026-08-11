# P2TR key-path accepts 65-byte signatures with sighash byte 0x00

**Component:** `rbitcoin-consensus` (`script/p2tr.rs` `verify_key_path`)
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — consensus accept-invalid
**Status:** fixed — consensus reject on shipped path (2026-08-10)

**Regression:** `rbitcoin-consensus` `script::p2tr::tests::key_path_rejects_65_byte_sighash_byte_zero`.
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

BIP341: a Taproot signature is 64 bytes (SIGHASH_DEFAULT implied) or 65 bytes with
a non-zero sighash type byte. A 65-byte signature whose last byte is `0x00` is
**invalid**. Bitcoin Core rejects this in key-path and tapscript paths.

rbitcoin’s tapscript path (`checksig_schnorr`) already rejects `sig[64] == 0x00`.
The key-path verifier maps the 65th byte through `TapSighashType::from_consensus_u8`
with **no** `0x00` rejection.

## Root cause

```text
p2tr.rs verify_key_path:
  65-byte → from_consensus_u8(sig_raw[64])  // no 0x00 guard
interpreter checksig_schnorr:
  if sig[64] == 0x00 { return Ok(false); }  // present
```

## Impact

Accept-invalid for key-path spends using `sig || 0x00`. Relay/mempool and block
validation share the script verifier, so both paths can diverge from Core.

## Suggested fix

Mirror the tapscript guard in `verify_key_path` before `from_consensus_u8`. Add a
regression that signs 64-byte Default, appends `0x00`, and asserts rejection while
plain 64-byte still accepts.

## Notes

Zip report: `2026-08-10-rbitcoin-p2tr-key-path-verification-accepts-65-byte-…`.
Audit confidence was medium on the rust-bitcoin `0x00 → Default` mapping; an
explicit reject is correct regardless of that mapping.
