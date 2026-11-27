Changed

- **A failed append refuses later writes to its files until reopen.** When
  a group of appends fails, a length may already be durable, so every file
  in that group refuses writes until the store reopens; reads continue and
  a flush may still republish the RAM length. This one rule replaces the
  separate latches in the block-filter, silent-payment tweak, and header
  tables. `header.head.mlt` now also refuses inserts after a failed length
  flush, instead of finishing that flush on the next insert.
