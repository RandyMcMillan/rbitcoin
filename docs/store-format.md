# Schema 17 store freeze

**Status:** `SCHEMA_VERSION = 17` is the frozen on-disk layout. This note is
the durable rationale: what 17 locks, what may change without a wipe, and
what would force 18. Byte layouts live in [`SCHEMA.md`](../SCHEMA.md).
History: [`SCHEMA_HISTORY.md`](../SCHEMA_HISTORY.md).

Writer / RAM policy on this binary (independent idx rolls, `strong_tx` always
L2, no `RWF_DONTCACHE`) is **not** a schema bump. Those are process
decisions over unchanged files.

---

## What 17 locks (on-disk)

| Object | Frozen choice |
|--------|----------------|
| Class A | Split `txout` / `inwit` / `spent`; thin LAYOUT17 meta; kinds **0–9**; 8 B spent slots; `spent.ovf` |
| Identity | Dense `txid.body` (32 B/fk); segmented `tx.head` (25-bit + fuse8 v2) |
| Idx | Per-stem `*.idx/` directories; **u32 stride-8**; hard span `2^32 × 8` ≈ 32 GiB; soft roll default 16 GiB |
| Class B | SH runs `key_len=40` unique `(sh, create_fk)`; megakey pages ULEB deltas (`ver=1`) |
| Class C | `confirmed[]` + `header_txs_*`; no `tx_height.body`; `strong_tx` bitset |
| Tweaks | Segmented `sp_tweaks.idx/` + `sp_tweaks.body/` (`off:u32`, body `0`/`33`) |
| Secret | `store.secret` XOR of scripts/witness; keyed `tx.head` mix |

Empty / leftover prior files may be unlinked or `meta` rewritten on open as
already listed in SCHEMA.md. A packed `tx.body` with creates, leftover
schema-16 SH catalogs, or 16-layout Class A with creates is **refused**.

---

## Writer policy (same schema)

### Independent idx rolls

Each Class A stem rolls its own idx when **that** stem’s next start would
exceed the soft span. `inwit` is the fat stem. A coupled roll used to open
new `txout.idx` and `spent.idx` segments whenever inwit crossed 16 GiB, so
the hot idx files split far earlier than their own windows.

Hot-set effect: fewer open `txout.idx` / `spent.idx` files and fewer
page-cache residents on pin / annotate. Inwit still segments at ~16 GiB —
that is the cold stem.

### `strong_tx` always L2

`is_confirmed_strong` is on confirm, reorg, and Electrum. One bit per create
is ~170 MiB at 1.4e9 txs and ~1 bit × N forever. `RBITCOIN_CLASS_C_INRAM_MAX_MB`
(default 256) still caps **`confirmed`** and **`header_txs_*`**. It does
**not** demote `strong_tx` to pread. At ~2e9 creates the old cap would have
turned every strong probe into a body read.

### No `RWF_DONTCACHE`

After the `spent` / `txout` split, annotate pwrites hit `spent.body` only.
`RWF_DONTCACHE` on those writes evicts **spent** pages, not `txout`. Spent
slots are 8 B and the next block’s annotate wave wants the same pages.
The flag is gone: no `dontcache_policy`, no `ReadOp`/`WriteOp` bit, no
ENOTSUP retry. Uring machines stay; SQE `rw_flags` stay 0.

Historical: schema 13 used DONTCACHE as a multi-target “drop after peek”
policy (cold head/idx/sidefile + body). That was already reduced to
spend-pwrite-only; the split made even that pointless. See SCHEMA_HISTORY
v13.

---

## New script kinds without a wipe

Kind nibble **10–15** is **Corrupt** on this binary (no implicit width).
That is the safety property: a future template must not be guessed.

A new consensus script type does **not** force a 17 datadir wipe:

| Path | On-disk | Old 17 binary | New binary |
|------|---------|---------------|------------|
| **RAW** | kind 0 + CompactSize + bytes | already decodes | same |
| **Soft-18** | new kind nibble + known width; `SCHEMA_VERSION = 18` | **refuses** 18 `meta` (or unknown kind) | reads 17 files; writes 18 |

Use RAW when the type is rare or the template is not worth a nibble.
Use soft-18 when the type is common enough to pay a width table. Soft-18
is **not** a silent in-place rewrite of 17 files: 17 binaries must not
mis-size a new kind, so they refuse 18. Operators stay on 17 until they
install an 18 binary.

Inwit `create_fk` Δfk (parked) is the same class: **18 or an inwit-only
rewrite**, not a silent 17 mutate.

---

## IBD hot set (census, tip 962 298)

Order-of-magnitude from SCHEMA.md. Pin / annotate / head-resolve need the
**hot** column; reconstruct / getdata also need `inwit`.

| File | ~GiB | Role |
|------|------|------|
| `txout.body` | ~103–111 (17 templates vs 15’s ~129) | Pin, SH, Cake |
| `spent.body` | ~21 | Annotate RMW |
| `txid.body` | 42 | Head-resolve identity |
| `tx.head/` | 8 | Txid → create_fk |
| `{txout,spent}.idx` | ~5 + ~5 (inwit idx is cold) | Range map |
| **Hot** | **~185–193** | Page cache we want |
| `inwit.body` + `inwit.idx` | ~486 + ~5 | Cold; `--datadir-cold` |

### Remaining size work (not 17)

| Idea | Why it waits | Est. |
|------|----------------|------|
| Inwit Δfk (`create_fk` relative to this tx) | Changes `inwit.body` bytes → 18 / inwit-only rewrite | Cold only; does not shrink the hot set |
| Drop 8-align pad on empty inwit / zero-out spent | Idx is u32 **stride-8**. Starts must be monotone and 8-aligned so a u32 relative still addresses 32 GiB. An empty record still occupies 8 B so `start(fk+1) > start(fk)`. Removing the pad means a different idx encoding (u64 abs, or a “empty” flag outside the start). | A few GiB across three stems; not hot-set material |
| Further `txout` templates | New common scripts: RAW or soft-18 (above) | Only if the type is frequent |
| `txid.body` compression / prefix | Identity peeks are random 32 B; prefix+fuse is a new side format | 18 if the file is no longer dense 32 B/fk |
| SH already thin | Run catalog is `(sh, fk)` only; vouts join Class A | Done in 17 |

Do not chase `inwit` size as an IBD **hot-set** win. Put it on a cold
volume.

---

## Field widths (10 years)

Assume ~400k–700k creates/day (today’s band through a busy bull). Ten
years ≈ +1.5e9…2.6e9 creates on top of ~1.4e9.

| Field | Width | Headroom |
|-------|-------|----------|
| `create_fk` / `header_fk` | u64 | 1e18-class; not a 10y issue |
| Spent `spender_field` | u56 | Same; 2^56 creates is not a Bitcoin problem |
| Height / `confirmed[]` index | u32 | ~1e6 heights now; 10y adds ~0.5e6; year 2106 is **timestamp**, not height |
| Idx relative | u32 × stride 8 | 32 GiB **per segment**. Soft 16 GiB rolls first. Independent rolls keep `txout`/`spent` on long segments. Mis-set `RBITCOIN_TX_IDX_SOFT_SPAN` above 32 GiB is a **writer** bug, not a width cliff |
| `tx.head` bits | 25-bit segments | Roll + seal; no mono-file widen |
| SH megakey page | 4 KiB delta stream | Page chain; not a single-integer cap |
| `sp_tweaks` off | u32 per segment | Already segmented |
| Script kind | 4 bits | 0–9 used; 10–15 reserved Corrupt; extension = RAW or soft-18 |

Nothing in the integer layout blows up on a 10-year mainnet. The practical
risks are **soft-span misuse** (one idx segment past 32 GiB) and **Bitcoin
timestamp 2106** (consensus, every node).

---

## Performance quirks (next ~2 years)

| Quirk | Status |
|-------|--------|
| Coupled idx roll over-segmenting hot idx | **Fixed** (this freeze): stems roll independently |
| `strong_tx` demoted at 256 MiB Class C cap (~2e9 creates) | **Fixed**: bitset stays L2 |
| `RWF_DONTCACHE` evicting `spent` pages the next block needs | **Fixed**: flag removed |
| Head insert via io_uring (~5× slower than page RMW) | Still **fd page-coalesce**; do not re-introduce uring insert without host A/B ([`io-modality.md`](./io-modality.md)) |
| Inwit on the hot volume | Operator: `--datadir-cold`. Not a format change |
| SH megakey materialize | Heartbeats + last-page append; still O(key) at bulk. No 17 change |
| Fused-resolve / spend-annotate uring machines | Keep. Do not flatten to one-shot `pread_batch` |
| `tx.head` 25-bit open segment | Load/seal policy already rolls; watch probe-depth warns, not a silent cliff |

---

## What would force schema 18

A **byte-incompatible** change to Class A / OA / body / idx / SH catalog
layout, or anything that cannot soft-open 17 files.

| Change | 18? | Notes |
|--------|-----|-------|
| New implicit-width script kind | Optional | RAW = no bump; nibble = soft-18 (17 refuses 18) |
| Inwit Δfk | Yes or inwit-only rewrite | Parked; cold stem |
| Idx not stride-8 / not u32 | Yes | Would retire the 8-align pad |
| Packed Class A again / merge stems | Yes | Wipe |
| SH `key_len` ≠ 40 or raw-u64 pages | Yes | 17 already refuses leftovers |
| `txid.body` not dense 32 B/fk | Yes | Soft-open only if dual-read is explicit |
| Fuse8 envelope v3 | No | Soft-migrate like v1→v2 (log + rewrite; no wipe) |
| Independent rolls / L2 strong / no DONTCACHE | No | Writer/RAM only |

Process (AGENTS.md): bump `SCHEMA_VERSION`, document SCHEMA.md +
SCHEMA_HISTORY.md in the same commit, refuse or soft-open with a one-line
operator message. Do not treat decode failure as “recreate the whole
table” unless the OA layout itself changed.

---

## Operator one-liner

Schema 17 is a **datadir wipe + re-IBD** from 16 and earlier (already
shipped). This freeze does **not** wipe again. Stay on 17 until a listed
18 change lands.
