Changed

- **Block-filter and silent-payment tweak puts keep no page mark.** Under the
  512-byte sector rule a put is an append: body and idx bytes are synced, then
  the body length, then the idx length. `store/guard/blockfilter` and
  `store/guard/sp_tweaks` are gone. A failed put still refuses later writes
  until restart.
- A first enable of either index that fails or crashes before it finishes, or
  a crash while a new segment pair is made, no longer leaves an index that
  refuses to open: the next start makes it again or drops the empty pair.
