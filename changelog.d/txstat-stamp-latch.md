Changed

- **A failed write or sync in a block-stats stamp stops the node.** The
  `getblockstats` stamp writes and syncs `txstat` files the Class A barrier
  also syncs. A failure there now latches Class A, as a failed barrier sync
  does, so no later barrier can succeed over pages the kernel dropped.
