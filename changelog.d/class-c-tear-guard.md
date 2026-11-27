Fixed
- **Class C and derived writes survive a torn in-place page.** A
  `header_txs` append rewrote the block holding the previous tip's committed
  range; both columns now arm one mark, so open restores the pair whole.
  `header.head` rewrote a 3 KiB chunk beside committed slots, and
  `txstat.body` stamped cells in place. A power cut could tear those pages, and
  open accepted a missing `header_txs` range as `Ok` or served a
  well-formed wrong stats or filter value. Those writes now arm a synced
  page-guard mark of the intended pages first and open restores it
  (`class-c-guard.md`); a txstat stamp is a one-sector write synced in
  place. A torn
  strong flush is repaired from the confirmed fence instead. `txstat.blk`
  and `txstat.ovf` are unchanged ([#2071],
  [#2072], [#2073], [#2075]).
- **A later write keeps outstanding page images.** Replacing a mark merges
  every image the new write does not cover; new images win on overlap
  ([#2108]).
- **Header and scripthash ingest heads become durable after their bodies.**
  `HeaderTable::ensure_batch` syncs `header.body` before `insert_many`.
  Scripthash ingest syncs slabs and the alloc header before
  `ingest_insert_many`.
- **Scripthash free-list links become durable before the allocator header.**
  Ingest, unlink, suffix free, and undo recovery sync the body after
  `free_slab` before `write_alloc_header`. An unlink batch syncs each body
  once after every free on that file, then writes its header once. Ingest
  rewrites in that batch share one `*.ahead` image; the home flush syncs
  the head file and then drops the sidecar. A batch that does not sync
  ingest leaves an outstanding ingest image in place. Covered ingest keys
  cleared for a packed shard share one head barrier. A full head-page
  cache syncs and drops its image before the next wave starts. `bump`
  and free heads must be in
  range, a popped link must be in range, and a slab's decoded count must
  match its head's `used`. A same-class slab rewritten in place can still
  pass that `used` check; copy-on-write is not in this change ([#2074]).
- **A served block filter is checked against its index hash.** A torn body
  no longer returns filter bytes that do not match the stored
  `filter_hash`. A by-height read that sees that mismatch recomputes the
  filter and writes it back when the hash matches the idx slot and that
  slot is still the row. A slot that moved is left alone. The read takes
  the replacement row, and errors if the row keeps moving. A by-hash read
  returns that error instead of a filter the idx rejected. If the height's
  block changed during the read, the by-hash read rebuilds the requested
  hash instead of returning the replacement row. It returns a stored row
  only when that row's idx slot still names the block and the body matches
  the slot. A block that leaves the best chain during the read counts toward
  the stale-block budget, including a budget of zero.
- A torn block-filter parent rebuild is still deferred ([#2109]).

[#2071]: https://github.com/rbitcoin-org/rbitcoin/issues/2071
[#2072]: https://github.com/rbitcoin-org/rbitcoin/issues/2072
[#2073]: https://github.com/rbitcoin-org/rbitcoin/issues/2073
[#2074]: https://github.com/rbitcoin-org/rbitcoin/issues/2074
[#2075]: https://github.com/rbitcoin-org/rbitcoin/issues/2075
[#2108]: https://github.com/rbitcoin-org/rbitcoin/issues/2108
[#2109]: https://github.com/rbitcoin-org/rbitcoin/issues/2109
