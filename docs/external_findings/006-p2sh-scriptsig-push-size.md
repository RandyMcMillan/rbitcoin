# P2SH legacy scriptSig pushes not limited to 520 bytes

**Component:** `rbitcoin-consensus` (`script/nested.rs`)
**Audit pin:** `2289578` (redteam zip 2026-08-10)
**Severity:** medium — consensus accept-invalid
**Status:** fixed — consensus reject on shipped path (2026-08-10)
**Found by:** redteam-ecosystem harness (static analysis / code-review)

## Summary

Bitcoin Core evaluates the P2SH `scriptSig` with `EvalScript`, which rejects any
push larger than `MAX_SCRIPT_ELEMENT_SIZE` (520 bytes) as a consensus failure
(`SCRIPT_ERR_PUSH_SIZE`). rbitcoin’s P2SH paths replace that evaluation with
hand-rolled push collectors (`split_script_sig_redeem`, `single_push_script_sig`,
`push_only_items`) that impose no element-size limit.

Consequences:

1. `scriptSig` pushes of 521+ bytes are accepted (Core rejects).
2. The classic 520-byte redeemScript cap (e.g. practical multisig size) is not
   enforced on the redeem push itself; only the redeem evaluation path uses the
   interpreter’s 520-byte cap on *its* pushes.

## Root cause

| Path | Behavior |
|------|----------|
| Bare / non-P2SH | `eval_script` enforces 520 (`interpreter.rs`) |
| P2SH nested / legacy | Collectors walk instructions with no size check |

Evidence anchors (audit): `nested.rs` `split_script_sig_redeem` / `verify_p2sh_legacy`;
contrast bare path in `script/mod.rs`.

## Impact

Accept-invalid: a mined spend of an oversized-redeem P2SH can put rbitcoin on a
temporary minority fork until the honest chain leads in work.

## Suggested fix

Enforce `MAX_SCRIPT_ELEMENT_SIZE` on every item collected from P2SH `scriptSig`
(shared helper so collectors cannot drift), returning a `PUSH_SIZE`-class script error.

## Notes

Zip report: `2026-08-10-rbitcoin-p2sh-legacy-scriptsig-pushes-not-limited-to-520-bytes-…`.
Opcode budget on scriptSig is secondary; element size is the consensus gap.
