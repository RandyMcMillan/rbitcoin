# Crash recovery and reorg semantics (store)

Hard kills (`kill -9`) and tip disconnects are normal. **Corrupt Class A / Class C
payloads are not repaired in-process** — reindex / redo IBD. Derived `tx.head` in
the **current** layout that is unreadable (truncated sealed MPHF, empty/short
`meta`, segment files with no `meta`, other open errors) **rebuilds from Class A**
when bodies exist. A crash during that rebuild leaves no `meta`, so the next open
wipes the directory and rebuilds again. Leftover
**prior layouts** (fuse8 v1, flat `tx.head.meta`) still **refuse**.

## Store integrity: what a crash can and cannot do

This is the guarantee the rest of this page implements. It assumes fail-stop
hardware and correct software. A crash is a stop, not a silent hardware fault.
A flipped historical byte is
[`106`](./external_findings/106-historical-bitrot.md), not a crash.

**Authoritative state.** Class A bodies, the Class C tip (`confirmed[]`,
`header_txs_*`, `strong_tx`), and the spend delta plus `spent.body` are the
store's own state. An index that can be rebuilt from Class A is not: `tx.head`
(from `txid.body`), scripthash (from Class A), and silent-payment tweaks (from a
Class A backfill). `header.head` is an index too, but it has no rebuild path
and is listed below. The mempool sidecar is separate: RAM is its source of
truth, open keeps a readable prefix, and Class A cannot restore an unconfirmed
transaction.

**What open trusts.**

- Small markers (`meta`, `tip_seal`, `spend_durable`, `class_a.synced`) are
  whole because each is a synced temp file renamed over the old one.
  `tip_seal` only lowers a tip; it never raises one.
- Class A keeps the highest create a durable `header_txs` range or
  `class_a.synced` names, not the last bytes it finds. An append writes past
  the committed end, so the sectors it shares with committed rows land
  unchanged, and the barrier is the stem sync. Open removes a leftover
  `tear_guard/` or `{stem}.kept`, which no release wrote.
- Class C `confirmed[]`, `header_txs_*`, and `strong_tx` share one synced
  `store/guard/class_c` mark of the intended 4 KiB pages before an in-place
  write. `header.body` only appends: rows, then length, each synced before
  the head insert. Only the first append arms `store/guard/header.body`, an
  empty flag, because open refuses an empty head beside body rows. One
  `header.head` generation uses `store/guard/header.head`. Its `.mlt` only appends, and the
  rows and length are synced before that mark arms. Open copies every named
  image before reading those files. The scripthash ingest head still writes a
  synced `*.ahead` image. An array flush always
  images page 0, because it rewrites the length there even when a shrink
  writes no slot or empties the table.
- A `txstat` stamp writes no image (the sector rule, below). Open refuses any
  entry named `txstat.body.ahead`, or one it cannot stat, instead of
  replaying it.
- `confirmed[]` has no null slot below the tip open keeps: the lower of the
  tip and the sealed tip, less a trailing null run. A binary without the
  image rewrote the 4 KiB block that holds that tip on every barrier, so
  Store open cuts the tip below a null in that block. It refuses a null
  anywhere else before it builds the fence: a torn page comes back from its
  image, so such a null is not crash damage. The fix is a reindex.
- The tip window re-reads every height above the durable marker `D`
  (`(D, tip]`) and at least the last six. A missing marker walks from genesis.
  A stored mismatch shrinks the tip to the last good height. An OS or
  completion-session read error stops open at that read and does not treat it
  as a bad block; a preceding trailing-null trim or `tip_seal` clamp may
  already be persisted.
- A fold over a page `spent.body` already holds is not trusted as found: the
  composed page is written to a synced `spent.delta/fold` before `spent.body`,
  and open copies that file again below the published end. The delta's own
  files are append-only and publish by a commit rename.
- `tx.head` rebuilds from `txid.body`; scripthash rebuilds from Class A on a
  detected corrupt read; silent-payment tweaks have a Class A backfill. Block
  filters have no read-triggered rebuild: a corrupt filter read returns an
  error to its caller. Scripthash damage that open cannot parse still refuses
  (see Thin scripthash).

**The guarantee, for the guarded state.** A crash leaves a prefix of a state
that existed: every row open keeps was durable before the crash, and the tip
is never advanced before the Class A bodies, headers, strong bits, and
`header_txs` range it names are written. The tip may be lower than the last
in-process tip; that is lost progress, not a wrong chain. Open does not invent
a height, a tx range, or a spend from a partially written block: it re-reads
the window, rebuilds a derived index from Class A, or cuts the tail.

## Two doors

**The sector rule.** The store assumes the device writes a 512-byte sector
whole or not at all, and that nothing below the filesystem re-encodes data in
larger units (for example dm-crypt with a 4 KiB sector on a 512-byte disk, or
a RAID 5/6 stripe without a write journal). The kernel writes a whole page
back, but a sector whose bytes did not change lands unchanged. A crash can
therefore leave any subset of the changed sectors new. A write needs no mark
when every such subset is a state open accepts. Two shapes always pass:

- An append: every byte is at or past the committed end, including bytes that
  share the committed tail block. It is synced before the length that names
  it is published, so a torn append is bytes open does not read.
- A write inside one sector, such as the 8-byte length at offset 8 or a
  `txstat` cell. It lands old or new. On a file whose commit a mark is
  protecting, it still has to be inside that mark's pages: it may belong to
  that commit, and open would roll the mark back around it.

**The small-write rule.** The sector rule covers a crash. A running process
has a second assumption: a write of a few bytes inside one sector is never
left half applied. `handle_pwrite_all` retries a short `pwrite` for the
bytes it did not write, so a cell can only stay half new if the kernel takes
part of it and the retry then fails, or the process stops between the two
writes. A crash there keeps the half-new cell on the device, so open relies
on this rule as well as the sector rule. The 8-byte length at offset 8 and
the `txstat` cells rely on it, and a half-new `txstat` cell still decodes.
Linux shortens a write at a file-size limit (`RLIMIT_FSIZE`) before it
copies, interior overwrites included, so the one software way to split an
aligned 8- or 16-byte field is a limit set to a byte offset inside it. A
limit in whole KiB, as `ulimit -f` and systemd `LimitFSIZE=` set it, cannot
split such a field, and the node sets none. A copy-on-write filesystem out
of space has not been shown to split one. A drive that tears inside a
sector can, and is outside the device contract below.

This is the assumption storage engines with a single-sector commit record
make and do not guard in software. PostgreSQL keeps `pg_control` within
`PG_CONTROL_MAX_SAFE_SIZE` (512 bytes) so one `write()` updates it, and
reports any short write as an error
([`pg_control.h`](https://doxygen.postgresql.org/pg__control_8h_source.html),
[ENOSPC](https://wiki.postgresql.org/wiki/ENOSPC)). LMDB writes its meta
page in place and relies on 512-byte sector atomicity, placing the txnid last
so a meta page cut short is ignored
([openldap-devel](https://www.openldap.org/lists/openldap-devel/201410/msg00004.html)).
SQLite assumes less: a sector write may be partial but is linear, and it
journals whole sectors instead
([psow](https://sqlite.org/psow.html),
[atomic commit](https://www.sqlite.org/atomiccommit.html)).

**The device the rules need.** Both rules describe an SSD that does two
things: it keeps every write a completed flush acknowledged, and it writes
each logical block whole or not at all when power fails. NVMe states the
second per command. Identify Controller reports the power-fail atomic write
unit as `AWUPF`, counted in logical blocks from zero, and a write of at most
that many blocks lands whole; every controller covers at least one block,
512 bytes or 4 KiB by namespace format
([NVMe atomic writes](https://blogs.oracle.com/linux/nvme-atomic-write-with-qemu),
[FAST '25](https://www.usenix.org/conference/fast25/presentation/jeon)).
The kernel writes dirty pages back in commands of one page or more, so a
command, including an append that shares its tail block with committed rows,
can be larger than that unit. The specification promises nothing for such a
command, and the sector rule then rests on the drive keeping each block it
carries old or new. A 4 KiB format makes each block a whole page; for a
command above `AWUPF` the sector rule still depends on the drive keeping
each block old or new, so the format is not a substitute for that behavior.
Prefer a 4 KiB format where the drive offers one. `nvme id-ctrl` shows `awupf`; `nvme id-ns` lists the formats, and
`nvme format --lbaf` changes it and erases the namespace.

The flush is where consumer drives differ. In a 2022 test that wrote,
flushed, and then cut power, the Samsung 970 EVO Plus 2TB and the WD Red
SN700 1TB kept every flushed write. The SK hynix Gold P31 2TB and the
Sabrent Rocket 512GB lost flushed data
([Tom's Hardware](https://www.tomshardware.com/news/sk-hynix-sabrent-rocket-ssds-data-loss),
[GIGAZINE](https://gigazine.net/gsc_news/en/20220224-ssd-cache-risk-data-loss/)).
A drive that loses flushed data breaks every rule in this file, not only
the sector rule, as it would for any database. Older SATA consumer drives
were also seen to tear a single block on power loss
([Zheng et al., FAST '13](https://www.usenix.org/conference/fast13/understanding-robustness-ssds-under-power-fault)).
Drives with power-loss protection, which most enterprise and data-center
models have, meet both requirements by design.

The spend delta asks less of the device than these rules do. It never
writes a 4 KiB block that holds bytes its commit names, so a block torn to
zeroes loses nothing ([107](./external_findings/107-torn-spend-page.md)).
That margin stays for the delta's I/O shape. The rest of the store does not
rely on it. On a device that can tear a block it did not finish writing, a
torn tail block can change committed rows an append shares it with, not
only cut them. Open catches that for `txid.body` rows in the blocks tip
revalidation rechecks (it recomputes each merkle root) and for header rows
near the tip (rehashed against the parent); elsewhere a changed row can be
served. A torn page 0 is refused only when its magic, schema or kind bytes
tore: a torn length that stays aligned and inside the file is trusted.

Any other write that changes committed bytes in more than one sector goes
through `page_guard`. The writer stores an image of every page that
write will touch, syncs that mark, writes the live bytes, syncs them, then
removes the mark. Open copies the images back while the mark is there, and
accepts the live pages only after the mark is gone. Restore copies each
image verbatim and does not check its kind, so the writer picks the
outcome: a backward mark holds pre-images and a crash rolls the write back;
a forward mark holds the finished pages and a crash completes the write.
Each mark's doc names its kind. The committed end is the durable header
length, not a RAM high-water.

A live mark owns every byte on the pages it images, appends included.
Restore writes whole pages, so an append that shares an imaged page rolls
back or completes with the mark, and it is durable only once the mark is
cleared. While the mark is live, the length that names such an append has
to sit on a page the same mark images, so it rolls back or completes with
the bytes. A leading header keeps that length at offset 8 on page 0, which
the sector rule above already keeps inside the mark. No length in a file
outside the mark, and no trailing footer past the committed end, may name
it.

**The marks.** `confirmed.body`, `header_txs_*`, and `strong_tx` use
`store/guard/class_c`. `header.body` is an append: rows, then length, each
synced before the head insert. Open indexes the rows the head does not name.
The first append alone arms the empty `store/guard/header.body` flag.
`header.head` uses `store/guard/header.head`. `header.head.mlt` is an append:
rows, then length, each synced before the head mark arms.
`spent.ovf` appends past its durable length and publishes the length only at
a spend checkpoint or flush, after the nodes are synced. A scripthash commit, including an
in-place slab and a head word, uses `store/guard/sh-*`. A `txstat` stamp
needs no mark: each cell and block slot sits inside one sector, and the stamp
is synced. Block-filter and silent-payment tweak puts are appends: bytes,
then lengths, body before idx, each synced, with no mark. A new segment
pair's names are synced before any length; Windows cannot sync a directory,
so there NTFS metadata journaling carries them. Open drops a newest pair
missing one file, and the index re-derives those heights. The mempool flush
appends its body the same way, then uses `mempool/guard/flush` for slots and
`meta`.
`tx.head` `*.mlt` is renamed into place, and the segment meta bit says the
file must exist. A strong bit a fence run names is set again by
`restore_strong_from_fence`.

**Not yet in effect.** The marks above are where this change series ends.
At this revision the write door checks armed pages, but no write path arms
or restores a mark. `confirmed.body`, `header_txs_*`, `header.head`, and
the scripthash ingest head still write the synced `*.ahead` image open
restores (What open trusts, above). `strong_tx` has no sidecar.
A scripthash slab or head word written in place has no page guard: the
`scripthash.dirty` refusal below and the rebuild cover an unclean stop. A
same-class scripthash slab rewritten in place can still tear
([#2074](https://github.com/rbitcoin-org/rbitcoin/issues/2074)), and an undo
restore of one head word can tear the neighbor slots on that page
([#2132](https://github.com/rbitcoin-org/rbitcoin/issues/2132)). Until those
are closed, the whole-store guarantee is the guarded state above, not every
row. Each mark takes over in the change that wires it:
`store/guard/class_c` in #2184, the header marks in #2185, `guard/sh-*` in
#2186, and `mempool/guard/flush` in #2188.

The scripthash index is still gated: an append, an unlink, or a deferred
empty-head commit writes `scripthash.dirty` before it writes. A full synced
`ScriptHashTable::flush` (file data, then the `scripthash.body`,
`scripthash.head`, and `scripthash.ovf` directories) removes the mark only
when no write started since its sync began, and never a mark that was present
at open or that a failed write left. A clean shutdown of a dirty index runs
that flush. The cold build and pack paths do not mark: an unsealed shard is
untrusted until its pack mark, and its keys stay in ingest or overflow until
then. A start with `--sh-index` that finds the file refuses and names the fix:
`RBITCOIN_SH_FORCE_REBUILD=1` (or the `scripthash.rebuild` and degraded
markers) wipes the index at open and removes the file, or start without
`--sh-index`. There is no clean-close gate and no whole-store snapshot, so a
Class B/C decode error after a crash can be crash damage rather than a
hardware fault: the scripthash path marks the index degraded and rebuilds,
while a Class A read error stops open. A design that needs one auditable
"closed cleanly" predicate, or a whole-store restore, needs a snapshot instead.

## Tip as commit point

Best-chain views ignore uncommitted Class C state:

| Write order (confirm **write** thread) | Role |
|-------------------------------------------|------|
| 0. Structural spentness / maturity / subsidy | No durable tip write yet |
| 1. `strong_tx` (L2 RAM) | May lead tip after kill **before** barrier |
| 2. Height-fence extend (RAM), then `confirmed[]` tip advance (L2 RAM) | In-process commit |
| 3. **`flush_class_c_tip`** (Class A `sync_data`, then headers, `strong_tx`, `header_txs`, `confirmed[]` last) | **Durability barrier**. IBD runs it on `rbtc-tip-flush` ([Flush](#flush)) |
| 3b. Scripthash write-behind enqueue | After tip. Direct IBD defers SH until tip entry |
| 4. Body-queue dequeue for those heights | Only after confirm-write returns Ok. In IBD the barrier may still be running |
| 5. Spend annotations (Direct) | After tip. A missing slot is unspent until open replays from the marker |

`is_confirmed_strong(tx)` ⇔ strong ∧ height fence contains the fk. Queries that mean “on best chain” use this (or equivalent). Confirmed-tx **API** readers pin that tip (`Query::pin_chain_view` / `still_live`) and retry on disconnect rather than pausing the writer — [`concurrency.md`](./concurrency.md#confirmed-tx-readers-pin--retry-not-a-lock).

On open (in order):

0. Store open restores `confirmed.body.ahead`, then cuts the tip below a null `confirmed[]` slot in the block that holds the sealed tip, or refuses a null slot anywhere else below it (What open trusts, above).
1. Soft `store/tip_seal` (if present): clamp confirmed tip that advanced without a complete barrier seal. This runs before the trailing-null trim, so a null run the clamp leaves on top is trimmed too.
2. **Tip-window revalidate** (Core `-checkblocks`, default 6, `0` = all): first drop any trailing null `confirmed[]` slots (HWM ahead of last real tip), then the last N confirmed heights — `prev_fk`/hash chain, `header_txs` range bounds, merkle root from `txid.body`, and parent edges on `input`. The walk does not reject a height for a missing strong bit: open sets every strong bit a fence run names before the walk, and again after a shrink. The walk logs `store: tip revalidate <lo>..=<tip>` before the first height and a progress line every 10 seconds. For each create in a checked block, `input` must be stamped, the edge count must match the `seqsigwit` input count (skipped under `--prune-seqsigwit`), the first tx must be the only coinbase edge, and every other edge must name an existing create with an in-range vout. A missing `spend_durable` file is the whole chain (`n == 0`): nothing has been device-flushed, and a matching last 6 heights is not a proof. A present marker widens any other window to at least 6 and far enough to cover `(D, tip]`. `checkblocks=0` stays the whole chain. On failure: clear bad Class A association and/or shrink tip to last good height, rebuild the fence, flush confirmed. A read that fails is not a bad block. An OS error or a completion-session fault (anything but a corrupt or missing record, or a stored length past the body read cap) stops open with that error and changes nothing, as Core stops on a block it cannot read. The next start walks the same window. A short new-layout `input` high-water mark is Class A count skew: open truncates the other stems to the stamped input count before this window, so a `header_txs` range past that count shrinks the tip. Open does not stamp `n_in = 0` onto `PREV_ON_INPUTS` creates. A legacy store with no `input` file, or a partial legacy tail whose next record still has an inline prevout, still backfills edges from `seqsigwit`.
3. After that walk, the same clear and restore run again, so a shrink does not leave strong bits off the final fence. `repair_class_c_above_tip` clears bits **not on the fence** via complement ranges (holes + suffix until a zero page) and does **not** walk every set bit. `restore_strong_from_fence` sets a bit a surviving run names. The clear logs `class_c repair cleared= ranges= ms=` even when zero.
4. Replay spend annotations for every height in `(A, tip]` (`spend_durable` annotated-through). A missing file has no device-flushed cursor, so `A = 0` and the chain is rewritten. `finish_post_commit_hashes` is idempotent. Then `sync_data` the replay inputs and publish `A` and durable-through `D` at the surviving tip. The replay logs height progress every 10 seconds.

Between the repair and that replay, open builds the in-process confirmed hash → height map when it is empty. A full `0..=tip` walk logs `query: height index` and a progress line every 10 seconds. Later tip moves extend or shrink that map.

In-process completion-session recover (IBD write/lookup/load/scripts and tip connect) consumes a 1000-height credit and requeues; it does **not** run `repair_class_c_above_tip` (lookup would race the write thread's strong-before-fence window). Abandoned leftover strong is the same as kill-9 and is repaired on the next open. Does **not** truncate Class A. Tip stays the commit point.

Open revalidation runs in `Query::open_or_create` **before** P2P can extend tip.

### L2 write-behind + body queue (phase 6)

- Compact Class C (`confirmed`, `header_txs_*`, `strong_tx`) mutate **RAM only** during the commit batch. The height fence is RAM-only (derived from those tables).
- **Connect** barrier order on disk (**tip last**): `strong_tx` → `header_txs` → **`confirmed[]` last**.
  - Mid-barrier kill after pre-tip tables: tip stays old; leftover strong not on the fence is repaired by `repair_class_c_above_tip`.
  - Never flush `confirmed` before strong on connect — tip with permanent unstrong txs (repair only clears leftover strong).
- **Disconnect** barrier order (**tip first** — opposite of connect):
  1. SH unlink only (do not unstrong yet).
  2. `confirmed` truncate + fence pop → `flush_confirmed_only` (durable tip shrink).
  3. Then `set_unstrong` → flush strong.
  - Mid-kill after tip shrink: leftover strong is **not on the new fence** → repairable.
  - Never unstrong while tip is still high — permanent unstrong-at-tip.
- Append-only tip extension writes **suffix only**. In-prefix full rewrites residual; tip-last connect + tip-first disconnect + BQ re-drive mitigate. A shrink flush writes a dirty kept prefix, then shortens the file. Disconnect does not store into that prefix, so its flush only shortens.
- Prefer **loss of uncommitted tip progress** over **tip-ahead-of-strong** or **tip-high-with-unstrong**.

## Class A (archive)

- Append-oriented; re-archive is **idempotent** when `header_txs` already present.
- **Never leads tip** on a clean run: Class A is published only on the confirm-write path with tip advance in the same era (no dual-track archive-ahead). A crash between the Class A write and the tip advance can leave bodies above the tip. IBD gets them again through the tip-hole cover.
- Kill mid-archive without a complete body association ⇒ not treated as archived; re-getdata. The locator header length is the create count. Body headers are written in that same commit, and the locator header is last, so a crash can leave a body header ahead of the count. Open truncates that body to the count's stem ends. A body header short of the locator count is the same unpublished tail: the connect barrier syncs Class A before Class C, so no committed row names it. Open keeps the largest create count every body header still covers, and a short `input.body` header cuts `input.loc` to the inputs it covers. The short-input probe runs only inside that covered prefix. An `input` stem whose count passed that prefix is cut to it. Open keeps exactly the creates a durable record names and cuts the rest without reading it. The floor is the highest create a `header_txs` range names, or `class_a.synced`, whichever is higher. Every record that names creates without a `header_txs` range (a spend delta commit, the scripthash `DONE.keys` / `DONE.post` marks, `include_hwm`) goes through `TxTable::sync_class_a_named` first. That call syncs Class A, then publishes `class_a.synced` by synced rename. Each Class A barrier syncs the stems before `class_a.synced`. An append writes past the committed end, so the sectors it shares with committed rows land unchanged and no page image is kept. A still-zero `input.body`, `txstat.*`, `seqsigwit.body`, or `spent.body` tail is data: a coinbase edge, an unstamped txstat cell, an empty seqsigwit pad, and an unspent slot are zero bytes. A failed write in an append (a body, locator, `txid.body`, `txstat`, or `input` row or header) fails that append and sets the latch a failed Class A sync sets: no later append, barrier, or mark runs, and the node stops. A mark therefore never names a create whose header missed, and open fits the archive on restart. Each record names `TxTable::settled_count`, not `count`: an append publishes its bodies and locator before `txid.body`, `txstat`, and `input`, and a spend checkpoint or scripthash pass running beside it must not name that unfinished tail. So every create through the floor is on the device, and nothing durable names one past it. A header can land without its rows, and a zeroed row can still decode (an all-zero input edge is a coinbase edge), so open does not read the tail to decide what to keep. The locators are cut to the floor before any other range read. A fit below the floor can only come from a binary without this barrier, or a device that lost synced writes. Freeing those numbers would hand them to later transactions under stale spends and scripthash postings, so open refuses and asks for a reindex. The `create.loc` and `seqsigwit.loc` opens refuse before their first cut, and the count repair refuses before it cuts a body, `txid.body`, or `txstat`. Two fits can land before the refusal: the locators cut to the floor, which drops only rows past it, and `input.loc` cut to its own body header. A store with Class C rows and no archive (a fixture) has nothing to refuse over. A `create.loc` count ahead of `seqsigwit.loc`, a short `create.off` / `seqsigwit.off`, a checkpoint longer than its locator, or an overflow row sitting past the overflow header is that unpublished tail and truncates to the last create those files still name. A failed read of the locator slot for a row past the overflow header stops open. `--prune-seqsigwit` stays on the last sealed tip, in tip mode and on the next IBD batch. Disconnecting a block that was never sealed does not move that tip. A short input or locator below a committed create is that same image and refuses. Trailing null `confirmed` slots are not that drop. A Class C image with a tx range and no archive is left in place.

## Spends (v5: annotation on create outputs)

- Sole spender: 8 B slot on **`spent.body`**. Multi: `MULTI_SPENDER` + `spent.ovf` list.
- Class A writes no `spent.body` bytes: an append moves the published end only, and the file is sparse. Every spend is a delta note, and a fold is the only `spent.body` writer; a page no fold wrote is never read and reads zero. A checkpoint seals the open layer under the exclusion confirm and disconnect take (after a disconnect, a note whose spender left the best chain becomes an explicit zero there), appends one record per touched page to the active `spent.delta/seg.<id>`, reclaims the sparsest segments under a live-share threshold of twice the delta's share of `spent.body` (at least 5%; any segment past 50%), folding records that spend at least half their page's unset slots into `spent.body` and carrying the others forward (each page's filled count is in the directory and only a fold changes it, so the decision reads no page), folds a touched page's record that already spends half its unset slots instead of rewriting it, appends its entries as one checksummed batch to `spent.delta/log.<id>` (or writes a new base `idx.<id>` once the log outgrows the old one), and renames `spent.delta/commit` before `spend_durable` advances. No write touches a 4 KiB block holding bytes the commit names: each generation's segment records and log batch start on a fresh block past the segment's header block, a base is never written once named, and no delta table header is rewritten, so a block a crash tears to zeroes loses nothing committed ([107](./external_findings/107-torn-spend-page.md)). A fold composes whole pages over their base, the `spent.body` page a fold wrote or zeroes, never a page the commit does not name, and only takes records sealed at or below the durable marker on pages wholly below the published end, never page 0. A page `spent.body` already holds goes through a synced `spent.delta/fold` first, so a page torn during the copy keeps every slot, including those only `spent.body` held; open copies a leftover `fold` again, below the published end ([107](./external_findings/107-torn-spend-page.md)). A page no fold wrote is written directly: nothing reads it until the commit. Folded segments are unlinked after the commit. RAM changes only after that rename, so a reader sees the sealed notes until the new entries replace them; a rename that landed before an error is installed too, and the next checkpoint syncs the directory again first. A crash before the rename leaves bytes past the committed segment and log ends, and maybe a new base pair the commit does not name; open reads the commit first, ignores the former, deletes the latter and every segment no entry names, and writes nothing to the directory, so nothing a crash left is selected later, even though the next generation reuses the crashed attempt's number. Open trusts the commit's lengths, not the table headers. Readers check the open/sealed maps, one directory entry per page, then `spent.body` only on a page a fold wrote; an explicit zero in the delta hides the body slot. A crash can cut blocks whose spends a commit already holds. Before replay and before any append, even when no block is left, node open runs `keep` over the pages written by generations whose sealed tip is above the recovered tip and lowers that tip, so a create that later takes a cut spender's fk is not read as spending the output. Open also rewrites the page the `spent.body` end straddles as a record over zeroes, so a fold's bytes past a cut end are never read for the creates that take those offsets again. A disconnect does not touch the delta. A schema-29 `spent.body` and `spend.pages` log become the delta's first generation on open, and the 29 files are removed after that commit. Superseded delta records are garbage, not read layers.
- Class A writes no spend slot. Every spend, one of an output created earlier in the same block or batch included, is annotated after tip as a delta note; a block connected again from the archive annotates its same-block spends the same way ([116](./external_findings/116-reconnected-in-block-spend.md)). A same-block spend of a parent this batch appended reads nothing: the append gives its range, and its slot is empty because the published end only moves back at open. A parent archived earlier reads its `create.loc` range and its slot, which can hold the note from the block's last connect.
- Annotations may remain after disconnect / for non-strong spenders.
- Best-chain spentness: annotation + `is_confirmed_strong(spender)`.
- Kill-safe for a stale annotation: non-strong fields do not false-positive if the filter is applied.
- A missing annotation is not kill-safe. Open replays `(A, tip]` from the block bodies, then advances `A` only after `sync_data`. `A` never exceeds the in-process tip. After a kill during an IBD barrier the file can name up to one write batch above the durable tip; open clamps it. Disconnect below `A` lowers `A`.
- A confirm write that fails after its tip commit (live index seal, spend annotate) leaves its first height pending in process. The next confirm write replays `[pending, tip]` before structural, and returns the replay error without validating if that fails. The spend snapshot does not advance past the pending heights, a disconnect lowers it to the new tip, and a snapshot note is capped at the confirmed tip it reads in the same atomic update. The checkpoint also never publishes `A` at or above a pending height.
- No `point.head` (v4 open-hash multimap removed).
- Class A is **three stems** (`txout` / `seqsigwit` / `spent`); bare-meta puts are rejected. Packed `tx.body` with creates is refused on open.

## Thin scripthash (Electrum outpoint pointers)

- Schema 18: the head value holds **one** inline create (`SH_INLINE_CAP`), a geometric **slab** (2–256 fks, ULEB128
  deltas), or a megakey **page chain** (≥257). Size-class freelist reuses freed slabs.
  Main heads are sealed MPHF+pack8 (no fuse); new keys go to global ingest OA.
- **No spend columns** — spentness is the `spent.body` annotation plus `is_confirmed_strong`.
- Direct IBD does not enqueue SH creates. Tip write-behind runs after `flush_class_c_tip`. Tip entry bulk-materializes from Class A.
- **Kill-safe without chain walks:** `scripthash.include_hwm` (and SEAL) advance
  only after the SH tables sync. Open re-queues every height above the highest
  one whose creates are all ≤ that floor. Replay is idempotent: per scripthash,
  append skips each `fk ≤` the last fk its head or last page already holds.
  Hot path only appends + maintains heads in RAM.
- Creates for unstrong / above-tip txs are **invisible** via `is_confirmed_strong`.
- Disconnect tip: **one unlink per scripthash**. A megakey drops that block's
  creates from the tail pages. The saved page image is synced before the undo
  record is published. The page the head names is updated before any earlier
  page is rewritten, then the head moves. Old pages are freed only after that.
  If clearing the undo fails, the saved page and the old head word are written
  back. A crash does the same restore, so the same disconnect can run again.
  A non-suffix unlink writes the replacement, syncs those bytes and the new
  heads once for the whole disconnect, then frees the old pages.
  A full-list unlink does not sync the empty head until the tip shrink is durable.
- A scripthash fault does not stop the chain and does not need a reindex. A `Corrupt` or I/O error from a
  disconnect's unlink batch, from a write-behind append, or from a history, listunspent or count read of a
  torn key marks the index **degraded** (a write-behind error that is not the SH tables', such as the Class C
  read of the job's height, still halts as before). `store/scripthash.degraded` is published like the other
  small durable markers (synced temp file, rename, directory fsync). Connect and disconnect go on with SH
  deferred. Every Electrum and Esplora address route answers 503 `scripthash index rebuilding`, and a read
  that overlapped the fault, or a disconnect's unlink batch (readers are held off for the batch), is refused
  too, because the reader's guard carries a generation that those events bump. A supervisor then waits for
  every in-flight reader (no timeout), wipes the SH tables under the appender lock, rebuilds from Class A, syncs
  the new head's `SEAL` and `scripthash.include_hwm` (files and their directories), re-arms write-behind, and
  only then removes the marker. A restart that finds the marker (or `RBITCOIN_SH_FORCE_REBUILD`) wipes at open,
  before tip entry, and keeps the marker until the cold rebuild at tip entry has sealed a head, so a kill in
  between wipes again. `RBITCOIN_SH_AUTO_REBUILD=0` leaves the index degraded until that restart.
  The in-place rebuild and tip entry's own SH step share one owner lock: tip entry waits for a rebuild that is
  running, takes the rebuilt head, and leaves a still-degraded index to the supervisor, so the two never write
  `scripthash.runs` and `scripthash.unsorted` together. The wipe also clears an unlink undo record left armed by
  a failed unlink, so the next open does not reject it.
  The marker rescues faults that surface after open succeeded: a torn page, a failed write or unlink, a read of a
  torn key. It cannot rescue damage that makes open itself fail (a truncated `scripthash.head/NN.val`, a torn
  undo record, or an alloc header whose bump or free head is out of range): open validates the SH files before it reads the marker, so such a store does not start, with or
  without `RBITCOIN_SH_AUTO_REBUILD` and `RBITCOIN_SH_FORCE_REBUILD`, and the fix is to stop the node, remove
  `scripthash*` (the marker included) from the store directory and start again; tip entry then builds the index
  cold from Class A.

## Flush

Clean shutdown: `flush_for_shutdown` runs the connect barrier (Class A `sync_data`, then Class C, incl. L2 dirty images), then async Class A, then one `tx.head` snapshot. A snapshot error is logged and does not skip those writebacks.
Steady path: payload pwrite + HWM publish; `sync_data` on flush barriers.
Kill mid-payload before HWM publish: readers never see past previous published length.

Connect barrier (`flush_class_c_tip`): Class A `sync_data` → headers (if dirty) → strong → header_txs → **confirmed last** → soft `tip_seal` (tmp + `sync_all` + rename + parent-directory `fsync`). The height fence is RAM-only and is not flushed. The Class A step is `TxTable::sync_class_a_data`: `txout`, `seqsigwit`, `spent`, both locator families (loc, overflow, checkpoint), `txid.body`, `txstat`, and `input`. Not `tx.head`. The sixteen syncs go to the configured bulk-IO session together (`bulk_io::sync_data_batch`): datasync `IORING_OP_FSYNC` ops in one io_uring submission, the shared `rbtc-io-pool` workers, or IOCP, which has no overlapped flush and flushes inline. In flight together, the filesystem can share journal commits and device flushes between them. With `RBITCOIN_IO=pread`, or for a file the session never submitted, they run one after another. Once a sync is submitted it may have taken the file description's writeback error, and a retry on that fd returns 0, so a failed batch reports every submitted sync it did not hear back from as EIO. The spend checkpoint runs the same Class A sync after it freezes the open notes and before it renames `spent.delta/commit`, so a commit never names a spender whose archive is unsynced (in IBD the spender can be strong on the RAM tip while its barrier is in flight). Class A syncs, and the spend fold's `spent.body` sync on the same fd, run one at a time under the latch's lock, so a sync that returns 0 because a concurrent one took the writeback error cannot commit after that failure. A failed sync fails the barrier before any Class C table is written, and latches: every later Class A sync (the barrier, the spend checkpoint, a scripthash collect, the table flush at shutdown, the `input` flush of the backfill and the prune release, and a `txstat` stats stamp, whose own failed write or sync latches too) refuses until restart, because a retried fsync can succeed after the kernel dropped the dirty pages. The first failure also stops the node (the hook `node_shutdown` registers): it exits non-zero instead of failing every later block, and the shutdown flush writes no Class C row. So no `confirmed[]`, strong bit, or `header_txs` range on disk names a create the device does not hold, and a Class A tail open finds past those rows is unpublished. `flush_header_archive` runs the same Class A step before its `header_txs` flush, and so does the mutated-body clear (`clear_archived_body_here`), whose flush writes every dirty range.

IBD runs that barrier on one long-lived `rbtc-tip-flush` thread for the confirm engine, so the next write batch's checks overlap it. The tip flush reads tails already covered by `published_len` and does not lock the append. It writes the inactive tear-guard slot and fsyncs that slot in the same Class A data sync. A page-0 image stores that loaded length, and open writes the slot length into that page in one write. Header range puts and the other Class C stores wait until that flush is joined. A structural reject after the body append still stores those header ranges after the join, so a retry sees `header_txs` and does not append the body again. Class C still flushes once per batch. Every `tip_commit` taker joins it first: the next commit, a disconnect or rewind, `Query::flush`, `flush_for_shutdown`, and dropping the `Query`. The order on disk does not change. A kill can lose one more write batch than in Tip mode. Its body-queue rows are already gone (RAM only, fetched again), and its spend annotations and filter or tweak puts may sit above the durable tip: open replays annotations and trims both indexes to the tip. A failed IBD barrier is not retried, because a failed `fsync` may have dropped the dirty pages. Every later commit fails until restart, so IBD halts after its one engine-fault retry. Tip mode runs the barrier inline.
Disconnect: confirmed truncate + `flush_confirmed_only` (also refreshes `tip_seal`) before unstrong/height clear. If the new tip is below `spend_durable`, that marker is lowered to the tip.

Confirm stores a snapshot height after annotations are written and does not `sync_data` these stems. Historical slots are delta records. The checkpoint writes them into `spent.delta` before the stem sync. `rbtc-spend-sync` does, about every 10 minutes: the spend checkpoint, whose freeze step runs `sync_data` on `spenders` and on Class A (not a high-water rewrite, not `tx.head`) before its commit is renamed, then if the confirmed tip is still at least that snapshot, publish `A = D` at the snapshot. A cookie inside a data file, or a clean page cache, is not this cursor: writeback is not a device flush, and the kernel may reorder data ahead of a later header write. Blocks appended above the snapshot during the barrier are flushed along and are not claimed. A disconnect bumps an in-process generation before it lowers the snapshot and the marker, including when no marker file exists yet. The checkpoint samples that generation before `sync_data` and again under the marker lock; it publishes only when the generation is unchanged, the tip is still at least the snapshot, and no annotate is pending. A disconnect and a reconnect of a different block at the snapshot height during that window leave the previous marker in place. An append strictly above the snapshot does not bump the generation, so the marker still publishes. One still pending caps `A` below its first height. A failed marker publish leaves the previous marker and does not start a new interval. Shutdown wakes the thread for one checkpoint, joins it, then runs `flush_for_shutdown`. The barrier is `File::sync_data` (Linux `fdatasync`, macOS `F_FULLFSYNC`, Windows `FlushFileBuffers`). It is not inside `flush_class_c_tip`, and it is not charged on the confirm thread's `ann_sync` timer. Open replay runs that same `sync_data` on the replay stems and `spenders` before it publishes the marker, including when the rewrite left those files clean.

| Write | Rebuild from durable tip + bodies? | Fsync |
|-------|--------------------------------------|-------|
| Class C barrier (`strong`, `header_txs`, `confirmed`, `tip_seal`) | No. This is the commit point. | Every barrier |
| Spend annotations (`spent.body`, `spent.delta/`, `spenders`) | Yes, from the block body and parent outputs, when those bytes are durable. Replay is only `(A, tip]`. Annotate never writes `spent.body`, and reads it only on a page a fold wrote. The checkpoint writes delta records, folds old segments into `spent.body`, and syncs the segments, `spent.body`, and the directory log or new base before `commit` and `A` advance. A schema-29 `spent.body` and `spend.pages` log become the delta's first generation on open, and `meta` moves to 30 only after that publish. A schema-30 store whose `spent.delta/` is gone refuses to open. [046](./external_findings/046-spend-durability.md) fixed the kill-9 gap where the marker advanced without the data sync ([107](./external_findings/107-torn-spend-page.md)) | `rbtc-spend-sync` about every 10 minutes, or early at 2,000,000 open edges, then advance `A` to the pre-sync snapshot |
| Class A bodies the tip window reads (`txid.body` for the merkle root, `input` edges, `seqsigwit` input count unless pruned) plus the strong bits | Class A bodies in the window are not rebuilt from the tip. A strong bit a fence run names is set from that fence; a bit the fence does not name is cleared. Historical bodies outside `(D, tip]` are not revalidated. `store.secret` is an XOR of scripts and witness plus a keyed head mix ([`concurrency.md`](./concurrency.md), [`SCHEMA.md`](../SCHEMA.md)). It is not a MAC of record bodies. Bit-rot of those bodies is an accepted reindex ([106](./external_findings/106-historical-bitrot.md)) | Same `sync_data`, which advances durable-through `D` to that snapshot. Open revalidates `(D, tip]` (at least the last 6) when the marker is present, and from genesis when it is missing. The window does not read `txout` or `txstat` |
| Scripthash heads, `tx.head` | Yes, from Class A | No barrier `fsync`. A corrupt record the write-behind apply reads (a torn slab after a power cut; recovery replays the jobs above `include_hwm`, which are the ones that tore) writes `scripthash.rebuild`. The apply is the single appender under `sh.appender`, so a reader racing an append never writes it. The next start treats the mark as `RBITCOIN_SH_FORCE_REBUILD` and removes it after the rebuild finishes; a crash before that rebuilds again. Write-behind runs only in Tip mode, after the inline barrier sealed the block; a failed Class A write latches and fails that barrier, so its postings name sealed creates. The bulk collect (`DONE.keys`, `DONE.post`) and the Class A tail backfill (`include_hwm`) `sync_data` Class A right after they sample the settled create count, so no marker names a create the device does not hold. An older binary did not, so its `include_hwm`, SEAL, or an unfinished collect's `DONE.keys`/`DONE.post` can name creates open then cut. Start, before any append can reuse a cut number, deletes the head and the unsorted collect when any of those marks is past the create count, and recollects from Class A. SH write-behind syncs the SH tables before it advances `scripthash.include_hwm` (a tip block, the end of a catch-up burst, or ≥1 s since the last advance); recovery replays the idempotent appends above that HWM. `sync=` on `tip: accept`. `tx.head` insert keeps the open tail in a private table and syncs `meta`, not those slots. A clean shutdown writes one dense snapshot of that tail (footer `generation` = the segment count) and fsyncs the image before it stores `generation`, then fsyncs that generation. A rebuild of an untrusted tail clears that segment's in-memory count and deletes the image before inserting, so a snapshot taken midway names only the rows stored. That write is about 1–2 s. A crash, or a snapshot whose `generation` is 0 or otherwise not that count, rebuilds the open tail from `txid.body` before probes are served. A full tail is about 10–20 s. An in-flight seal, which open already finishes from `txid.body`, adds about 15 s typical and up to about 30 s, so both together are about 25–50 s. The seal syncs its files before `meta` names it sealed. An IBD missing-parent verdict still scans `txid.body` from the oldest unsealed segment's first fk, read before the head read, and treats such a miss as `Corrupt` ([089](./external_findings/089-missing-parent-verdict.md)) |
| `txstat` fee/size cells, overflow blobs, `txstat.blk` slots | Yes, from the block (`getblockstats`, and a block size, weight or summary read that had to rebuild an active block, stamp it) | The IBD archive writes them as Class A, so the barrier syncs them before Class C names the block. The archive syncs `txstat.ovf` once per batch before it writes any slot. A later stamp writes and syncs the cells, then writes a nonempty blob and syncs `txstat.ovf`, then writes and syncs the slot. A stamped cell whose tail is missing, undecodable, behind a slot past the end of `txstat.ovf`, or on a create with no confirmed block reads as unstamped, so readers fall back and the next stamp writes it again. An IO error still fails the read |
| BIP-352 tweaks (`sp_tweaks.*`) | Yes, backfill from Class A | Each put syncs body, then idx. Open drops heights above the tip and fits the last record to its `n_tx` |
| Mempool sidecar | No. RAM is source of truth. Open keeps an in-range prefix and moves an unreadable image aside | 5 s path and shutdown `flush`: `sync_data` body, then slots, then meta |

`tx.head` meta, the spend marker, store `meta`, and `store.secret` use the same parent-directory `fsync` as `tip_seal` after tmp+rename. The `store.secret` temp is created mode 0600 (a stale temp is removed first) and the secret is never replaced. Windows denies that directory handle; the file was already synced. A missing `spend_durable` revalidates and replays from genesis. `checkblocks=0` still walks from genesis when a marker is present.

## Mempool sidecar (`{datadir}/mempool/`)

Private, **not** Class A. RAM graph is source of truth; files may lag.

- **New `meta`:** published by a synced temp file and rename, so a crash leaves no `meta` or a whole one. The unbroadcast list is replaced the same way.
- **Order:** `persist_due` and shutdown `flush` write the packed `tx.body` tail, `sync_data` that file, then write LIVE slots and `sync_data` those, then `meta`. `persist_due` `pwrite`s only new LIVE slot records (not the full table). A crash after the body sync and before the new slots loses those admits. Open skips a LIVE row whose `body_off + body_len` sits past the logical body and keeps the in-range prefix. Compact stays tmp+rename of packed images (reclaim). An admit compacts before it appends when dead payloads reach the live payload bytes, so a full pool that reuses the slot each eviction frees does not grow `tx.body` until a block.
- **5 s admits:** one `fdatasync` of the dirty body, then the slot sync, per dirty interval, from the tip-follow perf tick. Admits that missed that sync can still be lost (relay re-fetch). A lost admit can leave a durable child behind: a reorg re-admits a parent whose child was persisted while the parent was confirmed, and a crash before the next sync keeps the child alone. Tip-mode entry drops every reloaded tx with an input that is neither an unspent output on the active chain nor an output of a live pool tx, with its descendants (Core's reload rejects it as `missing-inputs`). A parent that only `tx.head` lost in the same power loss is still on the connected chain in `txid.body`: its child stays unless a block spent that output. `getblocktemplate`, `generate*` and the SV2 template provider select nothing from the pool until this purge has succeeded. Shutdown `flush` generation-bumps and uses the same sync order.
- **Unreadable image:** bad magic, a schema this binary does not accept, or an in-range payload that does not decode or whose txid does not match the slot. Open moves `meta`, `slots`, `tx.body`, and their `.tmp` files under `mempool/torn-<unix>/` and starts empty. `fee_history` and `fee_history.log` stay. An IO error (the mempool path is not a directory, or the open returns IO rather than corrupt, magic, or schema) still fails startup. Class A is untouched.
- **DEAD:** already-durable slots are a one-byte `pwrite` of the status. The owner of a removal (a block's confirmed txs and their conflict spends, an RBF, an expiry scan, a lone `remove_txid`) `fdatasync`s `slots` once, then writes `meta`. Open reloads from slot status and sets `live_count` from that scan, so a crash after the slots sync and before `meta` keeps the DEAD rows. A crash before the slots sync can resurrect those txs (same class as a lost admit: the body bytes are unchanged and still decode). An admit that never hit disk stays RAM-only (crash loses it). Do not dump the full slot table on strip — that would write LIVE rows whose body is still in the unpersisted tail. The many one-byte `pwrite`s dirty one 4K page per touched page; the following `fdatasync` writes those pages once.
- **Leftover schema 1–3:** convert on open (recode LIVE payloads, tmp+rename like compact). Every upgraded record takes the wall-clock second of that open as its accept time. Schema 1 has empty vin aux (SH reindex batch-fills); schema ≤ 2 has unknown sigops until the hub recomputes them; schema 3 keeps its sigop cost. Unknown schema takes the unreadable-image path above.

## Operator

- Direct IBD keeps segmented **`tx.head/`** (archive) and **spend annotations** (confirm) live; tip entry does **not** re-scan Class A to repair them. Corrupt head/spends ⇒ reindex (optional manual `backfill_tx_index` rebuilds segmented head mappings from Class A).
- **Segmented `tx.head`:** directory `tx.head/` with `meta` plus a shutdown snapshot `NNNNNN` of each dirty unsealed segment (`generation` = that segment's count); sealed `NNNNNN.mphf` + `.fuse8`. Packed BDZ `g` is FdOnly (4 KiB page stream); MPHF output is `rel−1`. Flat `tx.head.meta` **refuses** on open (wipe `store/tx.head`; Class A kept). Roll allocates the next private table and does not write it; seal runs on a sidecar and publishes later. Shutdown writes each dirty unsealed table once. A missing open-tail snapshot, or `generation` 0, rebuilds that tail from `txid.body`. Seal/install of `meta` / `.mphf` is sibling tmp + `sync_all` then rename (empty truncate of the live name is not a seal). Publish persists sealed `meta` before unlinking that segment's snapshot; a leftover file next to a sealed `.mphf` is discarded on open. Kill mid-seal leaves **at most one** unsealed non-tail and no snapshot (the private table died with the process): open collects fuse keys once from `txid.body` and seals it (does not retain `open_keys`). A process exit after sealed meta failed to persist writes that table; the next open loads it when `generation` equals the count. Two unsealed non-tails is **Corrupt**. A meta v1 head gets its slot fingerprints written on open: the tags are flushed before meta v2 is persisted, a slot still at fingerprint 0 matches every key, and a kill during the fill retries the fill on the next open rather than rebuilding. Leftover fuse8 v1 and flat `tx.head.meta` **refuse** (`Query::open`); wipe `store/tx.head` (Class A kept) then restart. Unreadable **current** head (empty/truncated `meta`, truncated/missing sealed MPHF) with Class A: wipe+rebuild from `txid.body` (same cost as a clean wipe). Wipe or empty occupancy + Class A: open rebuilds **MPHF+fuse8 directly** from `txid.body` in parallel (default **2²⁵** keys/range; `RBITCOIN_TX_HEAD_REBUILD_WORKERS`); no historical OA. Legacy mono `tx.head` file / `.new` / `.resize` are not opened — reindex.
- Scripthash: Direct IBD **defers** SH (no memtable, no confirm enqueue). After the horizon, two Class A `txout` scans: each worker owns a contiguous create-fk span and unsized maps (1.5 GiB estimate cap; spill the largest shard map while over budget; one writer, 1-slot queue) into `scripthash.unsorted/keys/NN/` (`SHKSP01` files, first-fk delta singles; tmp+rename, not a kill-9 barrier — no `DONE.keys` still wipes unsorted); merge folds those spills into one map, one walk to `scripthash.head/NN` + `multi/NN.fuse8`, and unlinks `keys/NN/`; then fuse-hit `post` (`SHPST01` spills under `post/NN/`), pack folds those spills then 2+ bodies, unlink each shard's extract as it seals. A **durable head** on restart stays Tip when an include mark or SEAL names a prefix: write-behind / `recover_sh_writebehind` fills the lag above that floor; leftover `scripthash.runs` are discarded (not WarmOnly-merged). A pack-complete head with neither mark and Class A creates still on disk is deleted and bulk-materialized. A height walk from 0 is not that rebuild.
  - **Full cold** when head empty. **`RBITCOIN_SH_FORCE_REBUILD=1`:** wipe head + full two-scan collect + pack. Empty collect after Class A creates remain is fatal.
  - **Cold resume:** the pack commit is `scripthash.head/NN.packed` (sibling of the MPHF base; holes stay). Open loads a shard's MPHF only when that mark exists. `MphfHead::exists` after pass-1 BDZ is **not** pack-done. A complete unmarked head (every shard has `.mphf`+`.val`, and `scripthash.unsorted` has no `DONE.keys` / `DONE.post` / `keys/` / `post/`) is soft-migrated: the marks are written, and a missing `include_hwm` is set from the create count. An extract still on disk never gets a mark, even if `include_hwm` already names the tip. No valid `DONE.keys` (including leftover `SHUNSRT3` / 24 B `NN`, or `keys/NN` / `post/NN` as a file) deletes unsorted and restarts pass 1. A spill whose first 8 B are not `SHKSP01` is Corrupt — wipe unsorted; do not parse or migrate. `DONE.keys` / `DONE.post` record the inclusive Class A `create_fk` scanned (`SHKEYS02` last_fk marker / `SHPOST02`). Missing current `keys/NN/` spills with unsealed shards finishes BDZ from those files. Head files present and no `DONE.post` restarts pass 2 from fk 1 (partial `post/` spills are deleted and recollected). Packing a shard soft-clears ingest keys that packed main now owns, so a tip append that landed on ingest while the shard was unsealed cannot hide that chain. A `post/NN` **file**, or a spill whose first 8 B are not `SHPST01`, is Corrupt (wipe unsorted). If Class A grew after `DONE.post` and **no** shards are packed, append postings. If any shard is already pack-marked, pack remaining unsealed `post/NN/`, then Class A tail-append onto the durable head (Direct write-behind no-ops). After all shards seal, the unsorted dir is removed.
  Empty head + leftover catalog: wipe leftover runs + SEAL, then Class A collect (not k-way from `scripthash.runs`). Durable head: leftover runs are discarded, `SEAL` kept; missing `include_hwm` bootstraps from SEAL. Inclusion HWM: `scripthash.include_hwm`. **Leftover live OA** at `scripthash.head` with no packed main shard, and a readable non-`SHSR` `ovf/NNNNNN`, are removed on open. A six-digit segment this open cannot read stays, and the next seal takes the id after the highest six-digit name. The body and a packed main index stay. `--sh-index` materializes the removed head. A first `create` writes `meta` last. `open_or_create` replaces a partial directory only when none of `txid.body`, `txout.body`, `header.body`, `confirmed.body`, `spent.body`, `input.body`, or `seqsigwit.body` already hold payload, on the hot directory or on the cold directory when the store is split. A file this open cannot parse still counts when any byte past its header is non-zero, or when that file cannot be read. Zeros past the header are empty-create slack. A missing `meta` next to that payload is `Corrupt` and is not wiped. An unreadable table (`EACCES`, `ENOSPC`) is not treated as an incomplete create.
  **SH head open:** a **main** shard with `NN.packed` loads its BDZ `NN.mphf`
  (`g` in RAM; tags stay on disk) and opens `NN.val` (no fuse, no `.idx`).
  **L0** `SHSR` ovf loads `.idx` (one entry per 170-record page) + `.fuse8`
  (BF8R). **L1** promoted ovf (newest `NNNNNN.mphf`) loads `.mphf` + `.val` +
  `.fuse8`. Occupancy scan is not used on those files. A schema-14 page-era
  durable index is **refused** (wipe `store/scripthash*` and rematerialize). Ingest OA still uses `{ingest}.occ`.
  A missing ingest file is empty. Open deletes a large ingest whose sidecar is
  0 and whose slots are empty, so a later start does not scan that hole.
