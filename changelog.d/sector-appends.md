Changed

- **Block-filter and silent-payment tweak puts keep no page mark.** Under the
  512-byte sector rule a put is an append: body and idx bytes are synced, then
  the body length, then the idx length. `store/guard/blockfilter` and
  `store/guard/sp_tweaks` are gone. A failed put still refuses later writes
  until restart.
