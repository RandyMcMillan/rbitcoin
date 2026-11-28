Fixed

- **A crash during the scripthash cold build no longer seals a shard whose
  body never reached the disk.** Sealing a packed shard wrote its
  `scripthash.head/NN.packed` mark before the shard body, its length, or
  its alloc header were synced. After a power cut the next start could
  trust chains whose bytes were lost, and a later append could reuse space
  those chains still occupy. The seal now syncs the shard body first.
