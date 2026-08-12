# Pending child not connected after a reorg makes it the tip’s child

**Component:** `rbitcoin-net` (`drain_pending`)
**Audit pin:** fuzzamoto report 009 / rbitcoin `8f3990f`
**Severity:** high — sync stall
**Status:** fixed — `drain_connects_pending_child_of_new_tip_after_reorg`
**Found by:** fuzzamoto

## Summary

`drain_pending` runs greedy tip-extend then `try_reorg_from_pending` **once**.
After the reorg, a pending block whose parent is now the tip is not attached
unless another peer message arrives. We can hold that block in RAM and sit one
behind forever.

## Fix

Loop greedy+reorg until tip hash stops changing (with 014 parent getdata). Red
on current HEAD; report’s f009 is not discriminating.
