Fixed

- **A failed step at shutdown no longer leaves `scripthash.dirty` behind.**
  With `--sh-index`, an error in an earlier store flush step (headers, the
  tip seal, a failed IBD tip flush or a missing keep-window file) returned
  before the scripthash sync. So did a failed node task after start, which
  skipped the whole shutdown tail. The next start then refused until a full
  index rebuild. Every shutdown step now runs, costliest first: the
  scripthash index, the chain tip, Class A writeback, the `tx.head`
  snapshot, the mempool, then the address book (`peers`). A failed step is
  logged and the next one still runs. The tip is still not sealed over a
  failed header sync, a failed IBD tip flush or a missing keep-window file.
  A disconnect whose tip shrink did not reach the disk keeps the mark, since
  the index then no longer matches the disk tip.
- **A store flush that fails at shutdown now exits non-zero.** It used to
  log a warning and exit 0. A task that fails after start no longer binds
  the SV2 listener on its way out.
