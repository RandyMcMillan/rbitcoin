# Design: Most-work reorg for IBD and tip-follow

**Status:** complete — selector, `accept_branch` tip restore, IBD BadPrev
classify/apply with held-by-hash side bodies + densify need, tip-follow
pending ≥128, multi-candidate re-rank after invalid, resume most-work scoring.

## Rule (Bitcoin)

Always follow the **fully valid** chain with **strictly most cumulative work**.
Divergence length is not a reason to stay on a weaker tip.

| Mode | Required reorg depth |
|------|----------------------|
| **IBD** | **Any depth** (DoS/RAM caps only) |
| **Tip follow** | **≥ 99 blocks** of divergence (`MAX_PENDING_BLOCKS = 128`) |

Header work ranks **candidates**. Block validation can disqualify a heavier
header chain; the true most-work-valid tip may be the current tip or a third
chain.

## Architecture (current)

```text
headers / inv / getdata / body queue
        │
        ▼
MostWorkSelector (LCA, path, sum_work, skip invalid-marked)
        │
        ▼
gather full Blocks for apply path (BQ / Class A / pending)
        │
        ▼
ChainHub::accept_branch
   work_better → snapshot old path → disconnect → connect
   on connect fail → restore pre-attempt tip; mark invalid; re-rank
        │
        ▼
IBD scrub / tip-follow pending clear / resume seed
```

| Component | Role |
|-----------|------|
| `most_work` | Pure LCA/path/`sum_work`/`select_most_work`/`InvalidHashSet` |
| `ChainHub::accept_branch` | Sole apply; tip restore on mid-branch connect fail |
| `ibd::reorg` | BadPrev classify; **held_bodies** by hash; awaiting densify; apply/re-rank loop |
| Body gather | BQ (height) · held-by-hash side bodies · Class A · BQ-by-hash |
| Tip-follow pending | Cap 128; assemble max-work fork from pending map |
| `resume_work_path_after_tip` | Child score = subtree header work, then depth; body tie-break |

Orchestration only (IBD main loop / peer session) — never confirm load/scripts
workers. One durable `confirmed[]` tip.

## Two layers

```text
# Layer 1 — candidate ranking (headers only)
prefer A over B iff sum(header.work() along A) > sum(header.work() along B)
  and no apply-path hash is invalid-marked

# Layer 2 — most-work *valid* (full blocks)
apply only if every block connects with full consensus.
On connect failure: tip restored; path marked invalid; re-rank other candidates.
```

## Invalid block on a heavier header chain

**Headers can overshoot true most-work-valid.**

```text
L = current tip, fully valid, work 100
M = peer header chain, work 150, connect fails mid-path
N = other peer chain, work 120, all blocks valid

Attempt M → fail → tip restored to L; M invalid-marked
Re-rank → Attempt N → tip = N
```

| Situation | Behavior |
|-----------|----------|
| M heavier, connect fails | Tip still L; mark invalid; re-rank |
| N valid, W(N) > W(L) | Apply N |
| No better valid candidate | Stay on L |
| Equal work | Do not switch |
| Soft corrupt wire (prev unknown) | Re-getdata only — not a reorg |

**Apply order:** assemble full path → `work_better` → `accept_branch`. Never
disconnect until bodies are gathered. Each `disconnect_tip` logs
`DisconnectTip` at warn. Mid-branch connect failure restores the
pre-attempt tip hash (snapshot before disconnect).

## IBD triggers

1. Confirm `BadPrev` at tip+1 with competing known prev (not unknown wire) →
   try sibling/winning path reorg before soft re-get.
2. Headers extend a side branch with tip work > best and bodies complete.
3. Selector + gather when candidate tips are ready.
4. **Disconnected most-work headers** (tip+1 / far work-path hash whose prev
   is not the tip): walk prev to the best-chain LCA, **getdata the shortest
   connecting prefix** that strictly beats tip work, then `accept_branch`.
   Do not wait for a child of the losing tip (BIP110-class: tip 961633, majority
   961632+). Explore tip is that prefix, not the header horizon.

## Tip-follow (≥99)

- `MAX_PENDING_BLOCKS = 128`.
- `try_reorg_from_pending` assembles fork branches, sorts by header work, applies
  best via `accept_branch`.
- Same invalid/restore policy as IBD.

## Resume / restart

`resume_work_path_after_tip` walks **tip → ancestors** (bounded). At each
parent it scores the on-path child’s subtree against **siblings** (strictly
greater header work, then depth; Class A body only breaks ties). The nearest
heavier sibling path is explored (not only tip descendants). Covers tip on a
loser **child** (mainnet 0139ed under 0169eb: better fork is d1e0 under
grandparent). Body preference alone must never re-elect an archived losing fork.

BadPrev densify walks **header prev links to the best-chain LCA** and
`need_getdata`s every missing mid body (not wire_prev alone).

Empty-headers with peer lag is **not** EOF when a heavier store path exists:
re-seed exploration and include greater-work tips in getheaders locators.

## Store / schema

Schema 14 supports apply (single `confirmed[]`, headers, Class A, disconnect).
Invalid marks are **process-local** for this implementation. A durable
invalid-by-hash schema bump remains allowed if restart thrash is proven.

## Historical note (why this exists)

Earlier IBD treated every `unexpected previous header` as soft re-get and resume
preferred Class A body first — a losing sibling tip could livelock confirm. That
path is replaced by the design above; do not reintroduce soft-only handling for
known competing prevs.

## Implementation discipline

Each vertical slice: **Red → surgical Green → light Refactor (still green) →
next step**. See agent plan / `docs/how-we-plan.md`.
