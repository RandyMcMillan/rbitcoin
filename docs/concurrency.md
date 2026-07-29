# Concurrency model (store / IBD / tip follow)

Short map of who may write which tables. **Format is unstable until 1.0.**

## Roles during IBD

| Role | Threading | Store writes |
|------|-----------|--------------|
| Peer IO (N tasks) | tokio multi-thread | none; decoded blocks **offer durable `store/block_queue/`** + **confirm feed (wire)** |
| Confirm **prep** | 1 OS thread | headers only; **plan** Class A (stamp create_fk, no body append) + pin parents once |
| Confirm **scripts** | 1 OS thread + rayon | **none** — pure CPU |
| Confirm **commit** | 1 OS thread | **sole Class A appender** + structural + Class C + spend annotate + tip GC; **`block_queue_dequeue_height`** |
| IBD main loop | 1 tokio task | none (orchestration only) |

**Height-ordered unified pipeline:** peer wire → prep (plan+pin+assemble with intake wire) → scripts → single commit era. **No** separate archive-lead track that appends Class A far ahead and reloads bodies for confirm. Optional archive job path remains only for unknown-height / legacy fallback.

**Wire retained:** batch creates use the peer `bitcoin::Block` through scripts; **no Class-A wire rebuild** on the unified path. Class A packed form is planned once and committed in the write stage.

**Durable queue:** `store/block_queue/` multi‑GiB payload FIFO (`RBITCOIN_BLOCK_QUEUE_GB` / `_BYTES`, default 8 GiB). **Offer** on peer Block; **dequeue** after confirm-commit. Restart rehydrates into the **same** confirm feed (wire), not a second pipeline.

**CreateResidency:** process-local fk/range/outs map shared for parent pin hits (raw FIFO). Prep seeds ranges/outs once per create on the pin path.

**tx.head overflow:** depth-exhausted inserts → `tx.head.overflow` (overflow-first lookup).

**Datadir secret (schema 12):** `store/store.secret` CSPRNG at create. XOR scripts/witness at rest; keyed TXID mix for heads.

## Roles after IBD / tip follow

| Role | Notes |
|------|--------|
| `peer_session` (split read/write) | Serve reconstruct + accept tip blocks via **`accept_and_connect_block`** → **`confirm_wire_run`** (same prep→scripts→commit) |
| Electrum | Read-mostly; joins Class A + scripthash under `Query` |
| Epoch finalize | Single-threaded control path; flushes mmap |

## Index modes (`IndexMode`)

| Mode | When | Spentness | Durable `tx.head` / spends | SH |
|------|------|-----------|----------------------------|-----|
| **Direct** | IBD (`enter_direct_index_mode`) | confirmed-strong annotations | commit-stage head insert; spend annotate in same stage | append-only target-sized runs + SEAL → bulk at tip |
| **Tip** | after IBD (`enter_tip_mode`) | confirmed-strong annotations | live heads + confirm spends | durable write-through after bulk |

Do not enter Tip until tip ≈ peer height. Tip entry bulk-materializes SH
(runs → fan-in reduce → durable tables); it does **not** rebuild `tx.head` or spend annotations.

## Locks (exceptions only)

**Default is lock-free** on table hot paths (see `AGENTS.md`):

| Mechanism | What it replaces |
|-----------|------------------|
| Map **epochs** (`TableFile`) | No map `Mutex` on read/write; capacity = new mmap window + pointer swap |
| Atomic `count` / HWM | Publish barrier (Acquire readers / Release appender) |
| Role exclusivity | One appender, one annotator — not a global store mutex |
| `tx.head` insert | **Sole writer**: plain Release store empty→fk + SeqCst fence per batch (no CAS). Role exclusivity — not multi-inserter safe |
| `tx.head` resize swap | Brief exclusive catch-up + rename (shadow fill unlocked; primary inserts pause via `head` write lock on final swap) |
| Process `rehash_gate` | Rare multi‑GiB open-hash rehash (host freeze prevention) |
| `ChainHub::confirmed` | `RwLock<HashSet>` for O(1) `has_block` (IBD assign path) |

There is **no** global “pause queries during confirm write.” Tip-as-commit +
`is_confirmed_strong` define query visibility ([`crash-recovery.md`](./crash-recovery.md)).

## Practical rules

1. Do **not** spawn a second Class A writer while IBD confirm commit is running.
2. Pipeline depth: prep(N+1) ∥ scripts(N) ∥ commit(N−1) via bounded load/write queues.
3. Scripts for batch N may run while prep does N+1 and commit does N−1. Scripts never touch disk.
4. On SIGINT, IBD cancels cooperatively — do not drop nested runtimes mid-await.

## Host freezes / IO storms

Single Class A writer is intentional. Multi‑GiB **mmap grow/remap** and **hash-head
rehash** (especially large **header** / scripthash head shards when materializing) can still stall the
**host** (page cache / disk). See **[ibd-io-audit.md](./ibd-io-audit.md)** for the
audit history, mitigations, and operator levers (`ionice`, dedicated disk, rehash
log lines).

### Confirm prep read pipeline

Cold parent `tx.idx` / `tx.body` on the **prep** thread uses
**mmap idx + bulk body** (`idx_body_pipeline` → `bulk_io`). Batch creates come from
**wire**, not a second Class A full-decode pass.
