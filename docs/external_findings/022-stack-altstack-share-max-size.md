# Stack size limit ignored altstack on data pushes (consensus split)

**Component:** `rbitcoin-consensus` (`script/interpreter.rs::push` / `OP_TUCK`)
**Severity:** **high — consensus split.** rbitcoin accepts a script Bitcoin Core
rejects (`SCRIPT_ERR_STACK_SIZE`).
**Status:** fixed — `push()` and `OP_TUCK` count **main + altstack** against
`MAX_STACK_SIZE` (1000), matching Core `EvalScript`.
**Found by:** floppy (differential fuzz vs Bitcoin Core)
**Regression:** `script::interpreter::success_and_disabled_tests::stack_and_altstack_share_max_size_on_pushdata`

## Summary

Bitcoin Core shares one `MAX_STACK_SIZE` (1000) across the **main stack and
altstack**. After every executed push (data push *and* opcode that grows the
stack) it checks:

```cpp
if (stack.size() + altstack.size() > MAX_STACK_SIZE)
    return set_error(serror, SCRIPT_ERR_STACK_SIZE);
```

rbitcoin had that combined check at the end of the **opcode** arm, but:

1. `push()` (used by `Instruction::PushBytes` and most opcode helpers) compared
   **only** `stack.len() + 1`.
2. `OP_TUCK` (`stack.insert`) compared **only** `stack.len()`.

`PushBytes` never reaches the opcode-end combined check. A script can fill the
altstack (e.g. 201 `OP_TOALTSTACK`, within the 201 op budget), fill the main
stack to a combined 1000 with `OP_1` (not an op-count opcode), then execute one
more **direct push**. Core rejects; rbitcoin accepted.

That is a chain split: a miner can include a tx Core considers invalid and
rbitcoin will connect it.

## Fix

`push(stack, alt_len, v)` rejects when `stack.len() + alt_len + 1 > 1000`.
`OP_TUCK` uses `stack.len() + altstack.len()` after the insert. Call sites pass
`altstack.len()` (scriptSig push-only uses `0`).
