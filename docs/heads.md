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

## Two policies that are not the same

| Policy | What it is | What it is not |
|--------|------------|----------------|
| **2-wave probe** | `sealed_age_from_index` vs `HEAD_PROBE_HOT_MAX_AGE` (3) | Page-cache |
| **RWF_DONTCACHE** | Spend-annotate **`spent.body` pwrite** only | Head/idx/load/append |

## Confirm stages (pointer)

| Stage | Head contact |
|-------|----------------|
| **lookup** | Pin by txid, then `tx.head` 2-wave + ID/idx. Stamps `create_fk` + range. |
| **load** | No head resolve. Pins `txout` by stamped range. |
| **scripts** | No store. |
| **write** | Sole Class A appender; `head_insert_many` for **new** creates. |

Roles and locks: [`concurrency.md`](./concurrency.md).
