# Agent notes

## Commit + release build after code changes

Whenever a turn **changes code** (or you finish a multi-step coding task in that turn):

1. **Commit** the working tree with a clear message (what + why). Prefer one commit per logical checkpoint — especially before starting a risky follow-on experiment, so we can roll back. Do **not** leave multi-hour IBD perf/refactor work uncommitted.
2. **Rebuild release** so the user’s binary matches the tree:

```bash
nix-shell --run 'cargo build -p rbitcoin-node --release'
```

Do the release build even if tests already ran in debug — the operator typically runs `target/release/rbitcoin-node`. Skip commit/build only when the turn was pure discussion / docs with no compile-affecting edits.

If you cannot commit (hooks, secrets, user said not to), still rebuild release and say explicitly that the tree was **not** committed.

## Datadir / store on this workspace (do not open in the agent VM)

The workspace is mounted into the agent VM as **9p** (`workspace` on `/home/agent/workspace`, `trans=virtio`). On this mount:

- **Writable shared `mmap` (`MAP_SHARED` + `PROT_WRITE`) fails with `EINVAL`** for store table files.
- Read-only mmap may work; `pread`/`pwrite` work.
- `rbitcoin-store` opens tables with `MmapMut` today, so **`Query::open` / `Store::open` / `rbitcoin-node` against paths under the workspace will fail** with `io error … Invalid argument (os error 22)` (e.g. on `scripthash.body`).

**Do not use the user’s live test datadirs in this VM** (e.g. `datadir-signet/`, `datadir-mainnet/`) to open the store, run the node against those paths, or diagnose tip stalls by loading Class A/C tables here. That includes ~27 GiB signet store under `datadir-signet/store/`.

### What works instead

- Read **logs** the user leaves in-tree (`signet-ibd.log`, etc.).
- Inspect store files with **non-mmap** tools (`pread`/Python struct parsing of HWMs, headers) when useful for offline forensics only — not as a substitute for a full node open.
- Reproduce with **synthetic fixtures** and `rbitcoin-test` scenarios under `/tmp` or other non-9p paths where mmap works.
- Ask the user to run the node / confirm diagnostics on their host (where the datadir is a normal local FS).

### Related symptoms already seen

- Tip stall / confirm diagnostics against `./datadir-signet` failed at open with mmap EINVAL.
- User-side IBD still advances; agent-side cannot drive or fully open that store.
