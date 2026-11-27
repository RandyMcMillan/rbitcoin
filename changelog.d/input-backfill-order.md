Fixed

- **The open-time input backfill syncs its rows before their lengths.**
  Upgrading a store with legacy inline prevouts rebuilds the `input.*`
  edges at open. Each append wrote its header length right after its rows,
  with no sync between them, and these creates are already synced Class A,
  so no later open cuts them. A crash could leave a length naming rows that
  never landed: zero bytes read as coinbase edges, or a zero locator
  refused open. The backfill now stages its rows in RAM lengths only. It
  syncs them, then publishes the body, checkpoint, and locator lengths.
