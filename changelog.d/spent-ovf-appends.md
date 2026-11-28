Changed

- **`spent.ovf` publishes its length only after its nodes are synced.** An
  overflow append writes past the durable length and moves only the RAM
  length. A spend checkpoint or store flush samples the length, syncs the
  nodes, then writes and syncs that length; an append that lands during the
  flush waits for the next one. A crash during the length write reopens at
  either length, and both name only synced nodes, so a torn tail is never
  read as the committed list. A failed sync still refuses later flushes until restart.
  No page mark is written.
