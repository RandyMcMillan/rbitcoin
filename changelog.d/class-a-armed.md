Changed

- **Class A keeps no page images.** Under the 512-byte sector rule an
  append leaves the sectors it shares with committed rows unchanged, and
  open keeps only the creates a durable `header_txs` range or
  `class_a.synced` names. A barrier now syncs the stems and writes no
  `tear_guard/` image. A leftover `tear_guard/` or `{stem}.kept` from a
  master build that imaged pages is written back to the stems, synced,
  and removed on open; one that does not decode stops open as corrupt and
  stays on disk. Store meta 31 is published by the
  first page-guard mark. A plain open of a schema 30 datadir leaves it at
  30, and an older binary refuses 31 `meta`.
