# Stranded on our tip when a peer reorganises

**Component:** `rbitcoin-net` (`has_block`, inv/headers download, `drain_pending`)
**Audit pin:** fuzzamoto report 003 / rbitcoin `8f3990f`
**Severity:** high — sync stall (not a validation split)
**Status:** fixed — `drain_requests_missing_parent_of_pending_branch`
**Found by:** fuzzamoto

## Summary

`has_block` can be true from a **RAM cache** (or a height later lost on reorg).
We then never `getdata` that hash again. A pending more-work branch whose parent
was never fetched cannot attach; the peer only announces its tip.

## Fix

Red on current HEAD first. If still broken: skip-download only when **connected**;
after drain, getdata missing parents of pending blocks; loop drain until tip
stable (with 020).
