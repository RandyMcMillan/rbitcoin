# Two more OP_CHECKSEQUENCEVERIFY divergences: NOP-instead-of-fail, and 4-byte operands

**Component:** `rbitcoin-consensus` (`script/interpreter.rs`)
**Commit:** `1ec7e42` (2026-08-07)
**Severity:** **high — consensus split** (both directions: one accepts what Core rejects,
the other rejects what Core accepts)
**Found by:** source audit while fixing [003](./003-bip68-version-signedness-consensus-split.md),
prompted by "check where the tx version is used"
**Status:** fixed — CSV fails below v2 after disable-flag; CLTV/CSV scriptnum width 5; allowlist 128/129 removed
not independently reproduced by a fuzz testcase

## A. OP_CSV is a no-op below version 2; in Core it fails

`crates/rbitcoin-consensus/src/script/interpreter.rs:723-727` (pre-patch):

```rust
0xb2 => {
    // OP_CHECKSEQUENCEVERIFY (BIP112). Pre-activation: NOP.
    // Core: also a no-op when tx.nVersion < 2 (even if softfork active).
    if !ctx.bip112_active || ctx.tx.version.0 < 2 {
        continue;          // <-- script continues, succeeds
    }
```

The comment is wrong. Core's `CheckSequence` **fails** the script
(`src/script/interpreter.cpp:1798-1801`):

```cpp
// Fail if the transaction's version number is not set high
// enough to trigger BIP 68 rules.
if (txTo->version < 2)
    return false;
```

and the caller turns that into `SCRIPT_ERR_UNSATISFIED_LOCKTIME`.

So a **version 1** transaction spending an output encumbered with `OP_CHECKSEQUENCEVERIFY`
is rejected by Core and accepted by rbitcoin. This needs no unusual version field at all —
plain v1 transactions reach it, which makes it more readily reachable than
[003](./003-bip68-version-signedness-consensus-split.md).

Core also evaluates the version gate *after* the operand's disable-flag check, not before,
so the ordering matters for operands with bit 31 set.

## B. CLTV / CSV operands decoded with 4 bytes; Core uses 5

`scriptnum_decode` enforces the 4-byte general arithmetic limit
(`interpreter.rs:1321-1324`) and was used for both locktime opcodes. Core reads them with an
explicit 5-byte limit (`src/script/interpreter.cpp:556` and `:584`):

```cpp
const CScriptNum nLockTime(stacktop(-1), fRequireMinimal, 5);
const CScriptNum nSequence(stacktop(-1), fRequireMinimal, 5);
```

Five bytes is what makes the full unsigned 32-bit locktime and sequence ranges expressible
as positive script numbers. With a 4-byte limit rbitcoin raises `scriptnum overflow` and
**rejects scripts Core accepts** — the opposite split direction to A and 003.

This is confirmed by rbitcoin's own vendored copy of Core's `script_tests.json`. Row #603:

```
sig="2147483648" pk="CHECKSEQUENCEVERIFY" flags=CHECKSEQUENCEVERIFY expect=OK
got=Err("script verification failed: scriptnum overflow")
```

`2147483648` is `0x80000000`, the sequence disable flag — Core decodes it (5 bytes), sees
the disable bit, and no-ops to OK.

**The CSV instance was latent**, masked by the version early-exit in A: with `tx.version < 2`
the opcode returned before ever decoding the operand, so the vector passed for the wrong
reason. Removing the early exit made row #603 fail immediately. The CLTV instance was never
masked and is live in the unpatched tree.

## Fix

Both are addressed in `target-patches/rbitcoin-tx-version-signedness.patch`:

* move the version gate after the operand disable-flag check and make it a script failure
  rather than a `continue`, comparing unsigned;
* add `scriptnum_decode_width(v, max_len)` and read the CLTV and CSV operands with
  `max_len = 5`, leaving general arithmetic at 4.

With the patch applied, all 203 `rbitcoin-consensus` tests pass, including the vendored Core
`script_tests.json` and `tx_valid`/`tx_invalid` vectors. The in-tree unit test
`csv_nop_when_tx_version_below_2`, which asserted the incorrect no-op behaviour, is renamed
and inverted to `csv_fails_when_tx_version_below_2`.

## Note on the existing allowlist

`crates/rbitcoin-consensus/src/script/core_tx_vectors.rs:460-461` allowlists two failures
with the comment *"CSV relative locktime edge not fully enforced"*:

```rust
("tx_invalid.json", 128, "CSV relative locktime edge not fully enforced"),
("tx_invalid.json", 129, "CSV relative locktime edge not fully enforced"),
```

These remained allowlisted after the patch and were not investigated. An allowlist entry
against Core's `tx_invalid.json` means rbitcoin accepts a transaction Core considers
invalid, so both are worth treating as candidate consensus splits in their own right.
