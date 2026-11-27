Changed

- **Class C flushes publish a forward page-guard mark.** `confirmed[]`,
  `header_txs` first+count, and `strong_tx` share `store/guard/class_c`.
  Open copies every named 4 KiB image onto its page before any read of
  those files, then removes the mark. A crash between the two
  `header_txs` columns cannot leave the pair split. Marks publish only
  after store `meta` is 31. A table fixture with no `meta` still arms.
  Creating `store/guard/` syncs the store directory. A `header_txs` pair
  whose two column lengths straddle the in-RAM cap refuses to open, since
  only a both-fd pair writes under one mark. Open refuses a leftover
  `confirmed.body.ahead`, `header_txs_first.body.ahead`, or
  `header_txs_count.body.ahead`. Scripthash `ingest.ahead` still replays.
