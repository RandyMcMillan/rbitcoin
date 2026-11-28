Changed

- **A mempool flush appends its body with no page image.** It writes the new
  `tx.body` records past the durable length, syncs, writes and syncs the
  length, and only then arms `mempool/guard/flush` for slots and `meta`. A
  crash before the slots leaves records that no LIVE slot names, which open
  ignores.
