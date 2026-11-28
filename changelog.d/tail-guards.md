Changed

- **A torn mempool page restores the committed bytes.** A mempool flush is one `mempool/guard/flush` mark for the new
  slots, durable deaths, and meta. `tx.head` seal always renames
  `NNNNNN.mlt` into place. The segment meta `mlt` flag says that file must
  exist; an older segment without the flag still loads a missing file as an
  empty map. Open refuses a leftover `txstat.body.ahead`.
