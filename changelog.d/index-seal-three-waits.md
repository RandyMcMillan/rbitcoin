Changed

- A confirm batch that seals filters and silent-payment tweaks syncs those
  appends in three waits: every payload, then both body lengths, then both
  index lengths. Payload bytes are durable before the length that publishes
  them, and a body length is durable before the index length that names
  those bytes.
