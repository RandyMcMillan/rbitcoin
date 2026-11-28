Changed

- **Class A keeps no page images.** Under the 512-byte sector rule an
  append leaves the sectors it shares with committed rows unchanged, and
  open keeps only the creates a durable `header_txs` range or
  `class_a.synced` names. A barrier now syncs the stems and writes no
  `tear_guard/` image. A leftover `tear_guard/` or `{stem}.kept` from a
  master build that imaged pages is written back to the stems, synced,
  and removed on open. The choice of what to write back is saved first,
  so an open after a failed write or sync finishes it. One that does not
  decode stops open as corrupt and stays on disk.
