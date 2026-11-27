Changed

- **`header.body` appends with no page images.** A header batch writes its
  rows past the end, syncs, then publishes and syncs the length before the
  head insert. Only the first batch arms `store/guard/header.body`, now an
  empty flag that lets open index rows beside a still-empty head. A failed
  append refuses later header puts until the store reopens.
- **Open indexes every unnamed row of the last header batch.** A batch
  now commits in chunks of at most 2000 rows. Open looks up each row of
  the last chunk, so a crash inside a head insert that named a later row
  no longer leaves an earlier one out of `header.head`.
