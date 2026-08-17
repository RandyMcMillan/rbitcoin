# Which head file is which

One map for `address_head` / `hashhead` / `sharded` / `segmented` / `scripthash_head`.
Confirm stages (lookup / load / scripts / write) and allowed IO live in
[`invariants.md`](./invariants.md). Roles: [`concurrency.md`](./concurrency.md).
On-disk bytes: [`SCHEMA.md`](../SCHEMA.md).

## Picture

```text
header.hash ──► header.head          HashHead (often 1 shard)
                    16 B prefix + 8 B fk; .mlt if multi

txid mix    ──► tx.head/             AddressHead inside SegmentedTxHead
                    4 B relative create id; fuse8 on seal

spk hash    ──► scripthash.head/NN   sealed sorted main after tip bulk (`SHSR`)
            ──► scripthash.ovf/ingest  incremental + post-seal new keys
            ──► scripthash.ovf/NNNNNN  sealed sorted ovf (ingest rolled)
```

## When to use which

| Module | On disk | Key → value | Who reads it |
|--------|---------|-------------|--------------|
| `HashHead` + `ShardedHashHead` | `header.head` | header **hash prefix** → header fk (`.mlt` if several) | Header ensure / `has_block` / prev walk |
| `AddressHead` + `SegmentedTxHead` | `tx.head/` (`meta`, `NNNNNN`, `.fuse8`) | **mixed txid** → relative **create_fk** (body-verify on `txid.body`) | Confirm **lookup** stamp after live pin miss |
| Sealed sorted SH main | `scripthash.head/NN` (`SHSR` + `.idx`) | Electrum **scripthash prefix** → slab/page locators | After tip bulk |
| Ingest + sealed ovf | `scripthash.ovf/ingest`, `ovf/NNNNNN` | Same key for incremental / post-seal new keys | Tip + never-bulk; lookup ingest → ovf → main |

`tx.head` is **not** a `HashHead`. `HeadRole` is only Header and ScriptHash.

## Lookup path (txid → create_fk)

1. Live pipeline pin by prev_txid (same Weak as outs).
2. **Open** wave: unsealed tail (age 0) — probe, two-shot `txid.body`, walk newest-first.
3. Unfinished keys: **sealed-hot** (ages 1..=3), same two-shot + walk.
4. Still unfinished or **unconnected** after those: **cold** (sealed ages ≥4).

`TipThenAny` / `TipOnly` still run later waves after an unconnected earlier
hit so a connected sibling in an older age can win.

## Three-wave probe (not page-cache)

`sealed_age_from_index` vs `HEAD_PROBE_HOT_MAX_AGE` (3) splits sealed-hot vs
cold. Open is its own wave. It is not an IO flag. `RWF_DONTCACHE`
is retired ([`SCHEMA.md`](../SCHEMA.md) Schema 17 freeze).

## Confirm stages (pointer)

| Stage | Head contact |
|-------|----------------|
| **lookup** | BQ-ahead TipOnly `get_fk_by_txid_batch` (same **3-wave** open / sealed-hot / cold; sealed-hot and cold only unfinished keys). Skip keys already in a still-queued layer (walk). Publish one `IdLayer` (`lo..=hi`) at wave end; drop a layer when no height in its span is still on the BQ. Combined `head_loc` cdf3 was ~90% on late-mainnet — not enough to pay a full-depth probe for every key. Revisit if leftover-split `wave` cdf3 is &lt;60%. |
| **load** | Stamp from in-flight + published union (`published.get()`, no mutex), then TipOnly leftover. Pins `txout` by stamped range or in-flight outs. Does **not** `spent.idx`-batch. In-flight holds planned creates until **after pin + scripts handoff**, when drain inserted the fk span **and** `covers_fk_span`. |
| **scripts** | No store. |
| **write** | Sole Class A appender **and** sole `spent.idx` stamper (`ensure_spend_abs_layouts`). `head_insert_many` write-behind. Drain ∥ Class C. Write queued is insert-only. In-flight is the RAM home until drain+fence after the next bind. RPC `get_fk_by_txid` hits durable head only until drain. |

Roles and locks: [`concurrency.md`](./concurrency.md).
