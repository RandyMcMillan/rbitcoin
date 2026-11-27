# Storage and indexes

## Drives

Run the datadir on an SSD that keeps flushed writes on power loss. NVMe
drives write one logical block atomically by specification. Whether a
consumer drive honors a flush varies by model: in a 2022 power-cut test the
Samsung 970 EVO Plus and WD Red SN700 kept flushed data, while the SK hynix
Gold P31 and Sabrent Rocket lost it. Drives with power-loss protection are
safe by design. A drive that loses flushed writes can leave a store that
refuses to open after a power cut, and recovery is a reindex. See
[`../crash-recovery.md`](../crash-recovery.md) (the sector rule).

## Bulk store IO backends

**Bulk batch** uses a **single** switch: `RBITCOIN_IO=uring|pool|iocp|pread` (default uring
when available). Table transport is always **fd pread/pwrite**. Compact Class C
is L2 write-behind; see [`docs/io-modality.md`](../io-modality.md). Per-path
env overrides are **removed**. If `uring` is selected but setup fails, demote to
**pread** / **pwrite**. If a live ring stops completing (`drain slow`, then abort),
restart with **`RBITCOIN_IO=pread`** — the process does not switch backends itself.

| Env | Values | Note |
|-----|--------|------|
| **`RBITCOIN_IO`** | `uring` \| `pool` \| `iocp` \| `pread` | Only bulk switch |

Inventory / survivors: [`docs/env-knobs.md`](../env-knobs.md).
Token meanings and ring depth: [`docs/io-modality.md`](../io-modality.md).
`RWF_DONTCACHE` is not used ([`SCHEMA.md`](../../SCHEMA.md) Schema 17 freeze).

## Defaults and memory budgets

| Knob | Default | Override |
|------|---------|----------|
| IBD concurrent getdata | **1024** | code `IbdConfig::window` |
| Blocks in transit / peer | **64** | `IbdConfig::per_peer` (16 MiB payload cap per peer) |
| Live IBD peers | **16** | `--max-outbound` |
| Inbound P2P sessions | **200** | `--max-inbound`. Peers that relay transactions hold at most half. At capacity, unprotected inbounds are evicted. Incomplete VERSION/VERACK is dropped after **60 s** (releases the slot). |
| Milestone (script skip) | mainnet anchor **966143**, testnet3 anchor **5128859**, signet **0**, testnet4 **0**, testnet5 **0**, regtest 0 | `--milestone` (`0` = full scripts; explicit height is height-only) |
| ConfirmParentCache header plans | always on | Tip-ahead header + tx_fks for multi-block MTP (no create pin FIFO) |
| Bulk store IO | **uring** (Linux) when available | `RBITCOIN_IO` only. Matrix: [`docs/io-modality.md`](../io-modality.md) |
| Archive Class A append | **pwrite** (always) | `txout` / `seqsigwit` / `spent` plus `create.loc` / `seqsigwit.loc` |
| `tx.head` (segmented) | fixed geometry | Default **25-bit**. Rebuild env: [`docs/env-knobs.md`](../env-knobs.md). Bytes: [`SCHEMA.md`](../../SCHEMA.md) / [`docs/heads.md`](../heads.md). Legacy mono-head datadirs require reindex |
| Confirm stages | **lookup · load · scripts · write** | Queues and pack/wave: [`docs/concurrency.md`](../concurrency.md). RAM: [`docs/ibd-memory.md`](../ibd-memory.md) |
| Confirm batch inputs | **8000** soft | Hardcoded. Live line: `h= n= in=` (**n** = blocks in pack, **in** = Σ inputs) |
| Mempool weight budget | **~300e6 WU** | `--mempool-size-mb N` (maps N×1e6 WU) |
| Inhibit auto-suspend | **off** | `--inhibit-suspend` (uses `systemd-inhibit` if available) |

### Suspend inhibit

Long IBD runs can be interrupted if the host auto-suspends. Pass
`--inhibit-suspend` to request a systemd **block** inhibit for `sleep` and
`idle` while the process runs (via `systemd-inhibit`). Default is off. If
`systemd-inhibit` is missing or logind rejects the request, the node logs a
warning and continues without inhibit.

**Peers file:** `{datadir}/peers` stores discovered addresses, **PeerFlags**
(connected / fast / slow / incompatible / last-fail), and each row's last-seen
Unix time between runs. Loaded at start (before seeds), updated after IBD and
on shutdown. Seeds are merged in without clearing known flags. New writes are
`rbitcoin-peers-v3` (IPv4, IPv6, Tor v3 `.onion:port`, and the row time).
`rbitcoin-peers-v1` and `-v2` files still load; their rows have no time.
A 0.9.x binary does not know the v3 header, starts with an empty book, and
its next save replaces this file. Copy `{datadir}/peers` before rolling back.

**Index modes:** Direct vs Tip: [`docs/concurrency.md`](../concurrency.md).
IBD finishes Class A + `tx.head` + spend annotations **before** tip; tip entry
does not backfill them. Scripthash: durable head stays Tip write-behind;
no head means Direct defers SH until a Class A collect + unsorted pack at
horizon. Confirm does **not** enqueue SH during Direct.

SIGINT keeps every sealed SH head; resume packs only unsealed shards (holes
stay; missing `DONE` restarts collect). `RBITCOIN_SH_FORCE_REBUILD=1` wipes the
head and does a full Class A collect + unsorted pack — unset after success
([`docs/env-knobs.md`](../env-knobs.md)). Missing `include_hwm` bootstraps
from SEAL (never clamp SEAL→0). Clearing residual run files **preserves
`SEAL`**. **SIGINT** mid cold keeps finished prefix shards
(`scripthash.cold_progress`). Materialize status logs ~**every 10s**.

On enter Direct, leftover `ibd_utxo.map` / `point.runs` / `tx.runs` from old
Catchup datadirs are removed — prefer a **fresh datadir**. Layout, shard
counts, ingest OA, and refuse lines: [`SCHEMA.md`](../../SCHEMA.md) and
[Schema upgrade](#schema-upgrade). Working-set sizes: SCHEMA census and
[`docs/ibd-memory.md`](../ibd-memory.md).

## Spend delta size

Schema 30 records every spend in `store/spent.delta/`, in 16 MiB
segments. Class A writes no `spent.body` bytes, so `spent.body` is a sparse
file: it holds only the pages a fold wrote, and `du` shows less than its
length. Each checkpoint reclaims segments with the smallest live byte
share while that share is under a threshold of twice the delta's share of
`spent.body`, and never under 5%: a delta at 10% of `spent.body` takes
segments under 20% live, one at 25% those under half, and past 50% any
segment. It never goes below two segments, and reclaims at most twice the
larger of the checkpoint's own record bytes and one 16 MiB segment, so a
delta above the threshold drains. A
record in a reclaimed segment is folded into `spent.body` once its spends
cover half the slots its page still has unset, so a page folds again each
time half of what is left gets spent; another record is copied forward to
the newest segment, which costs far less than writing a whole page. Past
50% every record folds. A page a checkpoint touches folds right away when
its committed record already spends half its unset slots, so a hot page
costs one page write instead of a rewrite of its whole record every
checkpoint. The decision reads no page: the directory holds each page's
count of filled `spent.body` slots, which only a fold changes. A fold onto
a page `spent.body` already holds, adopted from an older schema or folded
before, also writes it to `spent.delta/fold` first; one onto a page nothing
wrote goes straight to `spent.body`. Folds run in rounds of 4,096 pages and wait for the durable spend
marker. The directory is a base of 12 bytes per body page plus a log of 16
bytes per entry a checkpoint writes; once the log outgrows the base, a
checkpoint writes a new base. It costs 8 bytes per page in RAM (about 50 MB
for a 25 GB `spent.body`). A checkpoint runs every 10 minutes, or as soon as
2,000,000 slot notes are open. Open notes sit in one hash map by slot
index. Each checkpoint allocates the next window's map before the freeze,
sized from the window it closes: about 68 MiB after a window at the cap,
enough for the cap and a full write phase past it without regrowing, and a
few hundred KiB at the tip. It then sorts the frozen notes into an array of
16 bytes a note (about 32 MiB at the cap, plus a directory of 256 KiB)
that it reads and merges in page order, until the commit. While the sort
runs, the frozen map is still live beside the next window's map, so a
checkpoint at the cap peaks near 170 MiB of notes, and more when a failed
checkpoint left sorted notes behind. `notes_ram` is logged after the
commit and does not show that peak.

Every checkpoint logs two lines:

```text
store: spend delta gen=… new_record_bytes=… dense_record_bytes=… folded_segments=… carried=… carried_bytes=… durable=… folds_through=… reclaim_below_permille=… segments=… bytes=… live=… spent_body=… written=… records=… idx=… folded=… fold_records=… written_per_body_permille=… notes_ram=…
store: spend delta gen=… timing total_ms=… since_ms=… sealed_pages=… sealed_edges=… merge_reads=… merge_read_bytes=… merge_ms=… hot_folds=… hot_fold_record_bytes=… dense_kept_gen=… dense_kept_tail=… reclaim_read_bytes=… reclaim_read_ms=… folded_now=… dropped=… fold_rounds=… fold_compose_ms=… fold_file_ms=… body_write_ms=… body_sync_ms=… record_bytes=… seg_write_ms=… seg_sync_ms=… idx_entries=… idx_write_ms=… idx_sync_ms=… commit_ms=… install_ms=… unlink_ms=…
```

`bytes` is the delta on disk and `spent_body` the published `spent.body` length.
`dense_record_bytes` is the part of `new_record_bytes` for pages whose delta already
spends half their unset slots, rewritten whole because they are still hot.
`reclaim_below_permille` is the live share, in thousandths, under which a
segment is reclaimed at the delta's current size (1000 or more: any). `carried` and `carried_bytes` are
the records this checkpoint copied forward; they count in `records`.
`written` is every byte written for historical spend tracking since the
datadir was created (record bytes, directory bytes, and the composed pages a
fold wrote, to `spent.body` and, for a page a fold already wrote, to `spent.delta/fold` first), and `written_per_body_permille` is that total per thousand
bytes of `spent.body`. `notes_ram` is the RAM the open and sealed slot notes
allocate. Open logs the same totals once.

A checkpoint emits one compact INFO line:
`store: spend delta gen= since_ms= new_record_bytes= dense_record_bytes=
carried= segments= written= notes_ram= total_ms=`. The detailed `timing` line
(category `store`) is DEBUG for a fast checkpoint and INFO when `total_ms >=
1000` or `body_sync_ms + seg_sync_ms >= 500`. The `timing` line is one
checkpoint: `since_ms` since the previous one started, the sealed pages and
slot notes, the preads and bytes that read back the committed records they
merge into (records touching in a segment share one pread), `hot_folds`
(touched pages folded) and `hot_fold_record_bytes` (what rewriting those
records would have written), the dense touched pages kept because their
segment is newer than the durable marker (`dense_kept_gen`) or they reach the
body's end (`dense_kept_tail`), the bytes reclaim read from victim segments,
the reclaim folds (`folded_now`), records dropped because this checkpoint
rewrites their page, and the milliseconds of each step: composing folds, the
`fold` file, the `spent.body` writes and sync, the segment writes and sync,
the directory write and sync, the commit rename, installing in RAM, and
unlinking. Reuse `logging include ["store"]` to turn the detail on at a lower
level. `spend_room_wait_ms` on the `ibd: perf_full` line is the time confirm
waited at the 2,000,000-note cap for a checkpoint to start; the compact
`spend` object on `ibd: perf` also carries the window's checkpoint count,
worst `total_ms`, and `notes_ram`. After 60 failed checkpoints in a row (the sync thread retries
about once a second at the cap), that wait returns a store error and confirm
stops instead of stalling.

## Schema upgrade

Live bytes: [`SCHEMA.md`](../../SCHEMA.md) (`SCHEMA_VERSION = 30`). This section is
the operator copy-paste only — do not treat it as a second layout map.

Open **never silently wipes** a populated store (policy:
[`SCHEMA.md`](../../SCHEMA.md#changing-durable-bytes)). An older `store/meta` either
rewrites `meta` (payload-only) or **refuses** with a one-line message that
names the dirs. Corrupt files are **not** repaired in-process.

| Incoming `meta` | What this binary does |
|-----------------|------------------------|
| **30** | Open. `spent.delta/` records every spend; `spent.body` holds only folded pages. A datadir from an earlier build of schema 30 refuses with a wipe line. |
| **29** | Rewrite `meta` to 30, read `spent.body` once to take its pages as folded, publish them with a committed `spend.pages` log as the first spend delta generation, and remove the 29 files. `spent.body` is not written. No Class A rewrite. |
| **28** | Rewrite `meta` to 30 and sync the store directory, then read `spent.body` once to take its pages as folded, publish them with a committed `spend.pages` log, if any, as the first spend delta generation, and remove the 28 files. No table rewrite. A schema-28 binary then refuses the directory. |
| **27** | Rewrite `meta` to 30 and sync the store directory, then read `spent.body` once to take its pages as folded spend delta pages. No table rewrite. A schema-27 binary then refuses the directory. |
| **26** | Rewrite `meta` to 30 and sync the store directory, then read `spent.body` once to take its pages as folded spend delta pages. No table rewrite. A 0.9.0 binary then refuses the directory. |
| **25** | If `header.body` is still 96 B, rewrite each row to 88 B (`header.body.grow`, then rename), dropping trailing size and weight. An 88 B body is left alone. Then rewrite `meta` to 30. |
| **24** | Same header shrink as 25. Zero-extend `txstat.body` to the `create.loc` count (no `txout.body` rewrite). Unlink leftover `txfixed.body`. Rewrite `meta` to 30. |
| **23** | Headers stay 88 B. Rewrite `meta` to 30. `txstat.body` is created and aligned to the loc count when it is missing or short. |
| **22**, occupied Class A | Rewrite `create.loc.ovf` 12 B→16 B. Headers stay 88 B. Then the same `meta` rewrite and `txstat` align as 23. |
| **22**, empty Class A | Rewrite `meta` to 30, then open. |
| **Below 22**, empty or occupied | **Refuse.** One line: `schema before 22 refuses this datadir; wipe datadir and redo IBD`. Wipe the datadir and redo IBD. No per-version rewrite. |

On every open of meta 22–25 the node also renames leftover `inwit.*` to `seqsigwit.*` and `inputs.*` to `input.*`. A missing `input.loc` with seqsigwit prevouts backfills parent edges. A crash during the 96 B→88 B rewrite leaves the old `meta`; the next open retries the shrink. Size and weight after that come from `txstat`.

A `txstat.body` cell written as four ULEBs starting with `n_in` (an unreleased 25 experiment) is not detected and is not rewritten. Resync that datadir. A **26 binary** refuses 27 `meta`. A **25 binary** refuses 26 `meta`. A **24 binary** refuses 25 `meta` (do not downgrade in place). A **23 binary** refuses 24+ `meta`. A **22 binary** refuses 23+ `meta`. A **21 binary** refuses 22+ `meta`. A **19 binary** refuses 20+ `meta`.

When meta is below 22, the log line is:

```text
schema before 22 refuses this datadir; wipe datadir and redo IBD
```

A meta **22 or newer** datadir can still refuse leftover **index** layouts (fuse8 v1,
flat `tx.head.meta`, Shared file `scripthash.body`, pack8 Paged mode 10). The
line is one of:

```text
index refuses fuse8 v1; wipe store/tx.head and store/scripthash* then restart (Class A kept; tx.head rebuilds, SH rematerializes with --sh-index)
index refuses flat tx.head.meta; wipe store/tx.head then restart (Class A kept; tx.head rebuilds)
index refuses Shared (file) scripthash.body; wipe store/scripthash* then restart (Class A kept; SH rematerializes with --sh-index)
index refuses pack8 Paged (mode 10) scripthash heads; wipe store/scripthash* then restart (Class A kept; SH rematerializes with --sh-index)
```

Copy-paste (node stopped with SIGTERM):

```bash
DATADIR=/path/to/datadir
# fuse8 v1 / Shared SH body / Paged SH heads (same dirs as schema-20 index refuse):
rm -rf "$DATADIR/store/tx.head" "$DATADIR/store/scripthash"*
# leftover Class A `*.idx` dirs (schema 22 uses create.loc / seqsigwit.loc):
#   rm -rf "$DATADIR/store/txout.idx" "$DATADIR/store/spent.idx" "$DATADIR/store/seqsigwit.idx"
```

Keep Class A (`txout` / `seqsigwit` / `spent` + `create.loc` / `seqsigwit.loc`, `txid.body`, headers) and
Class C. Restart the same binary: `tx.head` rebuilds from Class A; with
`--sh-index`, SH rematerializes. Do **not** `rm -rf store/`.

**Kill-9 / crash is not a schema upgrade.** Open follows
[`docs/crash-recovery.md`](../crash-recovery.md) (tip-as-commit, Class C
repair above tip). Prefer SIGTERM ([`mainnet.md` catch-up](mainnet.md#catch-up)).
A corrupt file still means wipe/reindex — not an in-process repair.

## Scripthash index (`--sh-index`)

Class B **scripthash** reverse index is **optional** (default **off**), analogous
in *operator spirit* to Core’s heavy reverse indexes — **not** the same as
Core `-txindex` (we always keep Class A + `tx.head` for by-txid lookup).

| Mode | Behavior |
|------|----------|
| **off (default)** | No SH run enqueue during IBD; no tip bulk materialize. Tip follow + mempool relay + JSON-RPC work without SH. |
| **on** (`--sh-index` / `sh_index=1`) | Direct IBD defers SH. Tip entry runs two Class A scans, then pack and seal; write-behind follows. Address/scripthash Electrum/Esplora methods work after that seal. |

Electrum/Esplora **start without** `--sh-index`. Address/scripthash methods then fail closed (`scripthash index disabled`). Txid/outpoint/block/fees still work. Matrix: [`docs/lightning.md`](../../docs/lightning.md).

`--esplora-block-template` (conf `esplora_block_template=1`) enables
`GET /block-template` on the Esplora listen (same JSON as RPC
`getblocktemplate` template mode). Default **off** (404).

`--max-sh-creates N` (conf `max_sh_creates=`) defaults to **10000**. An unpaged
Electrum history or full stats join with more than N creates is refused
before Class A expand: Esplora **503** / Electrum JSON-RPC error
`scripthash join exceeds --max-sh-creates (<creates> creates, cap <N>)`. **0** is unlimited.
A create is a transaction that pays the script, not an output: the index
keeps one transaction key per create, so two outputs to the script in one
transaction count once. A height-ascending page is served when it closes
before reading every create. A newest-first page (Esplora's 25) of a script
with no more creates than the page limit is served. One with more creates,
at any cursor, and an Electrum `from_height` window take the same cap and
heavy-join turn as an unpaged join, checked from the create count before the
list is read. An explorer that must page a busy address above the cap needs a
higher `--max-sh-creates` or **0**; without that, every newest-first page of
that address is refused even when its newest rows are recent.

On the Esplora unix socket, `GET /internal/address/:address` and
`GET /internal/scripthash/:hash` answer the totals of `/address/:address` and
`/scripthash/:hash` (same JSON, `?asof=`, and chain-tip headers) with no
`--max-sh-creates`. They are for a trusted local process, such as an explorer
that reads a large script's totals once and then follows blocks itself. TCP
Esplora does not mount them, Electrum has no such method, and the public routes
on the socket keep the cap. Whoever can open the socket (mode 0660, owner and
group) can start these joins, so keep that group to processes you trust.
`--heavy-sh-creates` still runs them one at a time. There is no separate
ceiling for these routes. On one operator's signet node with the cap off,
totals for seven scripts with 22,000 to 1.9 million creates each took 0.3 s to
21 s and raised node memory by up to 0.6 GB. Time and memory do not follow the
create count: the largest took 8 s, a pay-to-anchor script with 77,000 took 21 s.

`GET /internal/scripthash/:hash/history` (and `/internal/address/:address/history`)
on the same socket is the confirmed `get_history` list with no
`--max-sh-creates`, for an explorer that copies a large script's whole list
once. It does not run the unpaged join. Once the response has left the
request timeout and its body is first read, it reads the script's creates and sorts them by height, then joins 4096 at a time and
writes each wave's rows before reading the next. For as long as the response is
open the node keeps that sorted create list (a height and a key, 16 bytes a
create), one wave, and the rows that belong to a block above the next unread
create (spends of older outputs); never the whole join or the whole list. Each
wave holds the `--heavy-sh-creates` turn and the store-read budget only while
it reads, not while it waits on the reader. A reader that leaves a chunk unread
for the listener idle timeout ends the response, and its body fails.

`--heavy-sh-creates N` (conf `heavy_sh_creates=`, default **10000**, **0** off)
lets one unpaged join with more than N creates run at a time across the
node. One join near a raised cap of 100000 was measured at 2.1 GiB peak RSS,
so several at once can push a small box into swap. A waiting join holds no
blocking-pool thread and no store-read permit: it waits on its request, then
runs again holding the turn for that whole request. Lighter joins and paged
history do not wait.

Order-of-magnitude costs (mainnet-class SSD; not a warranty):

- **During IBD with sh_index=1:** modest extra work (run stream); after IBD, bulk materialize is typically **tens of minutes to a few hours**.
- **Enable after tip already synced:** full recollect/materialize from Class A — **often multi-hour**; tip follow continues; Electrum waits until SH ready.
- **Disable later:** tables are **left on disk** (no automatic purge). Re-enable may rematerialize.

Tip-follow readiness is **independent** of SH materialize (`tip_follow_ready` ≠ `sh_tip_ready`).

### Abort / resume (tip materialize)

Keep **`store/scripthash.unsorted/`** until every shard has
`scripthash.head/NN.packed`. That mark is the pack commit. Pass-1
`.mphf`+`.val` without it is not sealed. Resume rules:
[`docs/crash-recovery.md`](../crash-recovery.md) (scripthash cold resume).
Extra disk during build is **`SHKSP01` spills** plus **`SHPST01` post spills**.
Restart with the same `--datadir --sh-index`. Do not delete unsorted files
to start over unless you intend a full Class A collect
(`RBITCOIN_SH_FORCE_REBUILD`). A spill whose magic is not `SHKSP01` or
`SHPST01` is Corrupt — wipe `store/scripthash.unsorted` and rematerialize.

| Stop | What restart does |
|------|-------------------|
| No valid `DONE.keys` | Delete unsorted and restart pass 1. Seal rewrites the MPHF. |
| `DONE.keys`, no `DONE.post` | Keep pass-1 MPHF; discard partial `post/` spills; pass 2 from fk 1; pack. Electrum stays down. |
| `DONE.post`, some `.packed` | Pack only the unmarked shards. |
| All `.packed`, include mark or SEAL | Tip write-behind from that floor. A second start does not collect. Electrum stays down until the floor covers the tip, then this process binds it. |
| All `.packed`, neither mark, Class A has creates | Delete the head and materialize from Class A. A block-by-block replay from height 0 is not the rebuild. |
| Complete head, no marks, no extract | Soft-migrate: write `.packed`. Missing `include_hwm` is set from the create count. |
| Kill-9 mid pack | Unfinished shard is redone. Open follows [`docs/crash-recovery.md`](../crash-recovery.md). |
| Write-behind IO error (disk full) | The process exits non-zero after the shutdown flush. Restart the same datadir; write-behind continues. |
| Corrupt SH (mixed body, refuse line) | Wipe `store/scripthash*` only, keep Class A, rematerialize with `--sh-index`. Leftover live OA is removed on open. |

Electrum waits until SH is tip-ready. Do **not** `rm -rf store/` for an SH
abort. Force-rebuild sticky env (`RBITCOIN_SH_FORCE_REBUILD`) must never redo
multi-hour Class A work casually — [`docs/env-knobs.md`](../env-knobs.md).

## Silent payment tweaks (`--sp-tweaks`)

Optional **thin** BIP-352 index for Electrum `blockchain.tweaks.subscribe`
(Cake Wallet, [kiss-bdk](https://github.com/kkdao/kiss-bdk); client-side
scan). Default **off**. The method still exists when off (naive per-height
walk). Flag on = persist + serve-from-index. Stream shape and Sparrow/Frigate:
[`COMPAT.md`](../../COMPAT.md) (Electrum surface).

IBD seals the index from the taproot origin when the flag is on before that
height, on the confirm write thread. With `--prune-seqsigwit`, a new index
can also start before taproot, or later once every height from that origin
through the dropped range is sealed or still in the last 288 blocks. Prune can be turned on
after the index is built when that index covers every height the cut drops.
A restart reseals a tweak gap from `seqsigwit.window` (RAM, then one read of
each spill file) before it advances the prune height. If that repair still
does not cover the height about to be dropped, startup refuses and leaves
the prune height where it was. Turning prune on while the index is short
says to stay unpruned with `--sp-tweaks` until the index catches up, or to
delete the index files. A datadir that is already pruned cannot turn prune
off, so that error says only to delete `store/sp_tweaks.idx` and
`store/sp_tweaks.body`. Sealed rows serve from
`sp_tweaks` plus `txout`. A pruned silent-payment scan and
`blockchain.tweaks.subscribe` read that index. An unsealed tip still in
the keep window is built from that window. A missing file, or a height
the index does not cover,
is an error. Neither falls through to the witness walk. Unpruned, a later
enable still backfills from `seqsigwit.body` on `rbtc-idx-wb`, and an enable
failure is logged while the node still starts.

On disk (schema 17 dirs; leftover single files are unlinked on startup):

| File | Contents |
|------|----------|
| `store/sp_tweaks.idx/` | `meta` (`origin` + fmt 3) + `NNNNNN` tip-only `u32` start offs (no `header_fk`) |
| `store/sp_tweaks.body/` | Matching `NNNNNN` files: per tx `len=0` or `len=33` + compressed `A_tweak`. New pair when the next start would exceed 4 GiB. |

**Not stored:** txids, Taproot outs, values, parent scripts. Notify
`output_pubkeys` are joined from this block’s **`txout`** body (~12 ms
sequential on a 4k-tx 9p block; witness stays in `seqsigwit`). Indexed serve does
**not** parent-peek (~40–80 blk/s vs ~1.5–3 naive on that VM).

Serve-time **`--sp-tweaks-dust SATS`** (conf `sp_tweaks_dust=`) omits P2TR outs
with `value <= SATS` and drops txs that then have none. Default **1000**.
`0` serves every value. **`545` matches Cake electrs** at `sp_min_dust` 546, which keeps
`value >= 546`. This is
not Core dust: P2TR at 1 sat/vB is about **330** sats; 546 is the P2PKH
figure Cake’s server used. The index is unchanged — only the Electrum JSON.

The builder is shared with `--block-filter-index`. Once the watermark covers
the tip, new blocks are sealed on the confirm write thread. Catch-up is one
IO thread reading windows of heights on one completion session
(`seqsigwit` and parent txids only for P2TR-output txs); one CPU thread
(`rbtc-idx-cpu`) publishes one job per height to `rbtc-scripts-*` (tweak EC,
and filter GCS when that index is on) and commits one batched height-blob +
idx write per window. A catch-up wave shares that pool with block scripts for
at most one window. Reorg truncates with tip. Kill-safe: `next_height` is the
last complete put. INFO every 10 s: `index: build next=… tip=… rate=…/s
remain=… read=…ms build=…ms commit=…ms`.

Cake Wallet’s scan isolate may still hardcode `electrs.cakewallet.com` even
after a successful probe — see `COMPAT.md`.
