Fixed

- **A crash during a scripthash append no longer serves another key's
  creates.** An append wrote a free-list link over a slab the durable head
  still named, and wrote a new slab over a free offset the durable alloc
  header still named, then synced both before the heads moved. After a
  power cut the index could read as valid and answer address and
  scripthash queries with the wrong transactions. An append now syncs a
  header that stops naming a reused offset before it writes there, and
  frees a replaced slab only after the head that named it is synced
  (#2145).
