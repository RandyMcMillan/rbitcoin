Changed

- **`header.head.mlt` leaves the `header.head` mark.** The multi-list only
  appends, so its rows and then its length are synced before the head mark
  arms. The mark images head pages only. A crash after the rows and before
  the head leaves unreferenced rows past the last id the head names. They
  are never read.
  A failed `.mlt` length flush refuses later inserts until reopen.
