Fixed
- **`confirmed[]` survives a torn page, and open refuses a hole in it.**
  Every `confirmed[]` flush rewrites the length in page 0 and the 4 KiB block
  that holds the previous tip, with no image. A torn page 0 failed open with
  `BadMagic`. A torn tip block zeroed committed heights; the tip-seal clamp
  could leave that null run untrimmed and shrink the tip onto a null slot.
  A null below the revalidation window was skipped by the height fence, and
  open then cleared the strong bits of that block's transactions. The flush
  now arms a synced page-guard mark of those pages first and open restores
  it (`class-c-guard.md`). Open clamps to the seal, compares the sealed hash, then trims null
  slots. Before it builds the fence it cuts the tip below a null in the
  block that holds the sealed tip (a crash under an older binary) and
  refuses a null anywhere else. An array flush images page 0 even when a
  shrink ends on a block boundary or empties the table, which covers
  `header_txs` too.
