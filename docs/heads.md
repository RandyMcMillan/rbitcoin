# Which head file is which

One map for `address_head` / `hashhead` / `sharded` / `segmented` / `scripthash_head`.
Confirm stages (lookup / load / scripts / write) live in
[`concurrency.md`](./concurrency.md). On-disk bytes:
[`SCHEMA.md`](../SCHEMA.md).

## Picture

```text
header.hash ──► header.head          HashHead (often 1 shard)
                    16 B prefix + 8 B fk; .mlt if multi

txid mix    ──► tx.head/             AddressHead inside SegmentedTxHead
                    4 B relative create id; fuse8 on seal

spk hash    ──► scripthash.head/     ScriptHashHead shards (64-way mainnet)
                    16 B prefix + 16 B value (inline / slab / page offs)
            ──► scripthash.ovf/      ingest OA + sealed overflow
            ──► sealed sorted main   after tip bulk (no main fuse)
```

## When to use which

| Module | On disk | Key → value | Who reads it |
|--------|---------|-------------|--------------|
| `HashHead` + `ShardedHashHead` | `header.head` | header **hash prefix** → header fk (`.mlt` if several) | Header ensure / `has_block` / prev walk |
| `AddressHead` + `SegmentedTxHead` | `tx.head/` (`meta`, `NNNNNN`, `.fuse8`) | **mixed txid** → relative **create_fk** (body-verify on `txid.body`) | Confirm **lookup** stamp after live pin miss |
| `ScriptHashHead` + shards | `scripthash.head/NN` + `.occ` | Electrum **scripthash prefix** → slab/page locators | Electrum/Esplora; IBD Direct writes runs, bulk at tip |
| Overflow + sorted main | `scripthash.ovf/*`, sealed sorted | Same SH key when ingest/main cannot hold inline | Tip SH lookup: overflow then main |

`tx.head` is **not** a `HashHead`. `HeadRole` is only Header and ScriptHash.

## Lookup path (txid → create_fk)

1. Live pipeline pin by prev_txid (same Weak as outs).
2. **Hot** wave: open segment (age 0) + sealed ages **≤3**.
3. `txid.body` identity + `txout.idx` for remaining keys.
4. Unfinished or **unconnected-hot** keys then **cold** wave (sealed ages **≥4**).

`TipThenAny` / `TipOnly` still run wave 2 after an unconnected hot hit so a
connected sibling in a cold age can win.

## Two-wave probe (not page-cache)

`sealed_age_from_index` vs `HEAD_PROBE_HOT_MAX_AGE` (3) decides which
`tx.head` segments are probed first. It is not an IO flag. `RWF_DONTCACHE`
is retired ([`store-format.md`](./store-format.md)).

## Confirm stages (pointer)

| Stage | Head contact |
|-------|----------------|
| **lookup** | BQ-ahead TipOnly `get_fk_by_txid_batch` (same **2-wave** hot then cold). Hits live on the BQ record. Combined `head_loc` cdf3 was ~90% on late-mainnet — not enough to pay a full-depth probe for every key. Revisit if leftover-split `wave` cdf3 is &lt;60%. |
| **load** | Stamp from BQ hits + in-flight / pins, then leftover: **pending snap (no fence)** then TipOnly (connected head). Pins `txout` by stamped range. In-flight holds planned creates until `covers_fk_span`. |
| **scripts** | No store. |
| **write** | Sole Class A appender; `head_insert_many` write-behind. Drain ∥ Class C. Snap is leftover home until **insert published and fence covers**. Forget after `drain.join()` + fence — never from Class C mid-insert (`67438`). Drain-first: snap stays until fence. Fence-first: still-queued keys stay until insert. |

Roles and locks: [`concurrency.md`](./concurrency.md).
