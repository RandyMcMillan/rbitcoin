# P2SH witness-program scriptSig exactness not enforced

**Component:** `rbitcoin-consensus` (`script/nested.rs`, P2SH dispatch in `script/mod.rs`)
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — consensus accept-invalid
**Status:** fixed — consensus reject on shipped path (2026-08-10)

**Regression:** `rbitcoin-consensus` `script::nested::tests::p2sh_nested_non_minimal_redeem_push_malleated` and related nested-witness exactness tests in `nested.rs`.
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

When the P2SH redeem is a BIP141 witness program, Core requires:

1. `scriptSig` byte-equal to the **minimal** single-push encoding of that redeem
   (`WITNESS_MALLEATED_P2SH` otherwise) — covers non-minimal PUSHDATA encodings
   and multi-push scriptSigs.
2. `VerifyWitnessProgram`: exact v0/20 → P2WPKH, v0/32 → P2WSH, v0 other length →
   `WITNESS_PROGRAM_WRONG_LENGTH`; non-v0 in P2SH → anyone-can-spend success
   (BIP341 is **not** applied to P2SH-wrapped v1).

rbitcoin recognizes only exact 22/34-byte v0 shapes as nested-segwit, accepts
non-minimal single-push encodings, and only partially pre-checks multi-push
malleation (P2WPKH-shaped last item). Unrecognized witness-program redeems with
an empty witness fall through to legacy eval and can accept.

## Variants

| | Shape | Core | rbitcoin (pre-fix) |
|---|--------|------|---------------------|
| (a) | Valid P2SH-P2W* with non-minimal redeem push | reject malleated | accept |
| (b) | Multi-push last = witness program + empty witness | reject malleated | often legacy accept |
| (c) | Single push v0 wrong-length program | reject wrong length | legacy accept |

## Root cause

`try_p2sh_nested_segwit` / `single_push_script_sig` do not require minimal encoding
or full `IsWitnessProgram` recognition; legacy fallthrough evaluates program bytes
as ordinary Base script.

## Impact

Accept-invalid and txid malleation of nested-segwit spends against nodes that trust
this consensus path; temporary minority-chain exposure if mined.

## Suggested fix

Recognize any witness program via `classify::witness_program`; require scriptSig ==
canonical minimal push of redeem; dispatch like Core; never legacy-eval a
witness-program redeem when witness rules are active. Keep multi-push **non-witness**
legacy P2SH (multisig) on the legacy path.

## Notes

Zip report: `2026-08-10-rbitcoin-p2sh-witness-program-scriptsig-exactness-…`.
Policy MINIMALDATA is not a substitute for this consensus gate.
