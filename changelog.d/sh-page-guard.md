Changed

- **A torn scripthash page restores the bytes from before that commit.**
  Each scripthash commit writes `store/guard/sh-<shard>` with every 4 KiB
  block it will dirty, then writes. Open copies the mark back before the
  body, the head, or a sealed overflow file is read. The ingest's
  occupied-count sidecar (`.occ`) is in that image only when the count is
  known. A large ingest opened without its sidecar has an unknown count,
  writes no sidecar, and so has none in its mark. A seal in the middle of a batch keeps later
  rows for a key that already lived in ingest on the sealed file. A torn
  `guard/sh-*.tmp` is not a mark. Marks publish only after store `meta`
  is 31. A table fixture with no `meta` still arms.
