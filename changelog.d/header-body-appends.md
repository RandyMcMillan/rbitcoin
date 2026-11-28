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
- **`header.head` rewrites slots under a forward mark.** One
  `header.head` generation uses `store/guard/header.head`; open copies it
  back before any head read. The first fill of an empty generation images
  and writes only the pages its keys land in. Open refuses a leftover
  `header.head.ahead` or `header.head.gN.ahead`, and only `meta` 24/25
  converts a 96-byte `header.body`; a later `meta` beside that length
  refuses to open.
