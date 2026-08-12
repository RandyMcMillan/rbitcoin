# Compact-block reconstruction places the same transaction twice

**Component:** `rbitcoin-net` (`compact.rs` `try_reconstruct` / blocktxn apply)
**Audit pin:** fuzzamoto report 007 / rbitcoin `8f3990f`
**Severity:** high — sync stall (remote-triggerable)
**Status:** fixed — `repeated_short_id_is_requested_not_duplicated`
**Found by:** fuzzamoto

## Summary

Two slots can match one mempool candidate (repeated short id). We already mark
**ambiguous multi-candidate** slots missing; we do **not** reject **repeat
placement**. The reconstructed block has duplicate txids, CheckBlock fails, the
announcement is consumed, and we never getdata the real block.

## Fix

Track placed txids; second use of the same tx → missing (getblocktxn / full
getdata). Same uniqueness on the `blocktxn` apply path.
