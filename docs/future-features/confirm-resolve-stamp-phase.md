# Confirm: split lookup into resolve + stamp (parked)

Parked from the “script is the serial pole” plan (2026-08). **Not** in the
active worktree branch — implement after the in-machine lookup cuts and write
dirty/append/head write-behind land.

**Why:** Fat-era `ibd: perf` (~872k) showed lookup as the pole (~171k µs/blk,
almost all `head_fk`) while `scriptq_hwm=2/4` and script wait ~2.7 s. Body
queue already holds ~290 blocks. Resolving parents of **BQ-ready** heights
before stamp claims a batch is a new confirm **phase**, not a helper inside
today’s lookup.

**Target pipeline:**

```text
resolve (BQ-ready) → stamp (claim + attach + remainder) → load → scripts → write
```

Today lookup = structure + `plan_batch` (head resolve + stamp) on the **claimed**
batch only.

## Constraints (same as confirm-pipe plan)

- Not a process create-pin FIFO. Results live with the BQ entry; drop BQ
  (reorg / reject) drops resolve results.
- One TipOnly `get_fk_by_txid_batch` call site.
- BIP30: newer unconnected must not hide older connected.
- `ibd: perf` shows `resolve=` / `stamp=` (or equivalent), not one `lookup=`
  that hides both.
- Start with **one** resolve worker; a second only if host still starves
  `scriptq`.

## Spike then implement

1. **Spike:** name offer vs claim vs stamp; queue (`resolveq` vs lookup thread);
   AGENTS.md inventory tokens.
2. **Implement:** resolve stage on BQ-ready heights; stamp attaches hits and
   only head-resolves leftovers. Slim `/tmp` tests: resolve-then-stamp has
   empty remainder; BQ drop forces re-resolve; format_info has new tokens.

## Related

- [Class A storage pack](./class-a-storage-pack.md) — disk size, not this rate work.
