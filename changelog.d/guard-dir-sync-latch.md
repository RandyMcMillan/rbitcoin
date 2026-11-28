Changed

- **A failed `guard/` sync now latches until restart.** After a page-guard
  mark change whose directory sync fails, the files it dropped refuse
  writes, new marks, and a mempool compact until the node restarts. A later
  sync that succeeds no longer releases them, because it cannot show the
  earlier unlink reached the device (#2209).
