# Design: Most-work reorg for IBD and tip-follow

**Status:** implementing (see plan steps in agent session).  
**Problem class:** mainnet 2026-08-09 tip stall — confirmed losing fork at 961632; soft `BadPrev` livelock; resume prefers Class A on loser.

## Rule (Bitcoin)

Always follow the **fully valid** chain with **strictly most cumulative work**. Divergence length is not a reason to stay on a weaker tip.

| Mode | Required reorg depth |
|------|----------------------|
| **IBD** | **Any depth** (DoS/RAM caps only) |
| **Tip follow** | **≥ 99 blocks** of divergence (raise pending-body caps) |

Header work ranks **candidates**. Block validation can disqualify a heavier header chain; the true most-work-valid tip may be the current tip or a third chain.

## Two layers

```text
# Layer 1 — candidate ranking (headers only)
prefer A over B iff sum(header.work() along A) > sum(header.work() along B)

# Layer 2 — most-work *valid* (full blocks)
apply only if every block connects with full consensus.
On connect failure: tip restored; path marked invalid; re-rank other candidates.
```

## Failure modes today

1. **IBD** treats all `unexpected previous header` as soft wire re-get → livelock when tip is a losing fork.
2. **`resume_work_path_after_tip`** prefers `has_body` then higher fk → restart re-selects archived loser.
3. **Tip follow** `MAX_PENDING_BLOCKS = 64` blocks ≥99-block reorg assembly.
4. **`accept_branch`** exists but is underused by IBD and under-fed by tip-follow caps.

## Architecture

```text
headers/inv/getdata/BQ → candidate trees + bodies
         → MostWorkSelector (LCA, work_better, skip invalid)
         → gather full Blocks for apply path
         → ChainHub::accept_branch (work check → disconnect → connect)
              on connect fail → restore prior tip; mark invalid
         → IBD scrub / tip-follow pending clear
         → resume policy (restart)
```

| Component | Responsibility |
|-----------|----------------|
| Header graph | Parent/children, path, LCA |
| MostWorkSelector | Rank non-invalid candidates by header work |
| Invalid set | Failed apply hashes (process-local first; durable schema OK if restart thrash) |
| Body gather | BQ / Class A reconstruct / pending / getdata; **no disconnect until complete** |
| Apply | Solely `accept_branch` with tip restore on failure; **one** `confirmed[]` tip |
| IBD scrub | After **successful** reorg only |
| Resume | Prefer deeper/more-work child chain; body is tie-break only |

## Invalid block on a heavier header chain (critical case)

**Headers can overshoot true most-work-valid.** Seeing a greater-work *header*
chain is **not** enough to adopt it. Full block validation can disqualify that
path; the actual most-work **valid** tip may be:

1. our current tip **L**, or
2. a **third** fully validated chain **N** that is heavier than L but lighter
   than the (invalid) header chain M.

### Worked example

```text
L = current tip, fully valid, work 100
M = peer header chain, work 150, bodies gathered
    → connect fails at LCA+3 (script / spentness / BIP34 / …)
N = other peer chain, work 120, all blocks valid

Correct outcome:
  Attempt M → connect fails
    → tip restored to L (never leave torn tip at LCA)
    → mark invalid hash(es) on M so we do not thrash re-applying M
    → re-rank remaining candidates (skip invalid-marked)
  Attempt N → W(N)=120 > W(L)=100 → accept_branch succeeds → tip = N

Incorrect outcomes (bugs):
  - tip ends on M (invalid won)
  - tip stuck at LCA after failed connect (torn)
  - forever soft-BadPrev / soft re-get treating M as “corrupt wire”
  - never try N after M fails (selector assumes “headers said M, stuck on M”)
```

| Situation | Required behavior |
|-----------|-------------------|
| M has W(M) > W(L); connect fails at block k | Tip still **L**; mark block k (and candidate tip) invalid; prune M from ranking |
| N valid, W(N) > W(L) | **Apply N** next (full validate) — not “stay forever waiting on M” |
| No remaining candidate with W > W(L) | Stay on L; continue getheaders/getdata |
| Equal work to L | Do not switch |
| Soft corrupt wire (prev unknown) | Not a heavier-chain case — re-getdata only |

**Apply order:** assemble full path → work_better → `accept_branch`. Never
disconnect until bodies are gathered. On mid-branch connect failure after
disconnect: **restore pre-attempt tip hash** (snapshot old path before
disconnect). After restore + invalid-mark, **re-run selector** over remaining
candidates — do not assume the first heavy header tip is the only option.

### Layer handoff

```text
MostWorkSelector ranks by header work, skipping invalid-marked tips
        │
        ▼
gather bodies for chosen path M
        │
        ▼
accept_branch (work check → disconnect → connect)
   success → tip = M; scrub; done
   fail    → restore tip L; mark invalid; loop selector (maybe N, else stay L)
```

Selector is **headers only**. “Most-work valid” is only known after successful
`accept_branch`. Invalid-heavy is therefore a **selector + apply loop**, not a
single shot.
## Store / schema policy

Schema 14 already supports apply (single `confirmed[]`, headers, Class A, disconnect). **Schema bump is allowed** (e.g. durable invalid-by-hash) if restart thrash is proven by a red test. **Do not** invent multi-tip Class C / parallel candidate confirmation chains.

```text
1. Correct online with confirmed[] + headers + Class A + BQ + RAM candidates? → do that.
2. Restart re-livelocks invalid heavy paths? → SCHEMA invalid-by-hash.
3. No multi-tip confirmed without a failing test that proves (1) is insufficient.
```

## IBD triggers

1. Confirm `BadPrev` at tip+1 with competing known prev (not unknown wire).
2. Headers extend a side branch with tip work > best and bodies complete.
3. After getheaders, candidate tip work > best and apply path gatherable.

Orchestration thread only (not confirm load/scripts workers).

## Tip-follow (≥99)

- Raise `MAX_PENDING_BLOCKS` to ≥128.
- Assemble full pending branch to parent-on-best-chain; pick max-work fork; `accept_branch`.
- Same invalid/restore policy as IBD.

## Mainnet class (961632)

| Fact | Response |
|------|----------|
| Losing tip at H | Candidate = parent of BadPrev wire = sibling of tip |
| Real H+1 prev is winning H | Apply path = [winning H, real H+1, …] once bodies exist |
| Soft re-get loop | Competing path ≠ corrupt wire |
| Resume body preference | Fix child selection |

## Non-goals

- Offline datadir surgery as primary recovery.
- Soft-requeue of store invariants.
- Equal-work tip thrashing.
- Multiple durable confirmed tips.

## Implementation discipline (every step)

Each vertical slice follows **Red → surgical Green → light Refactor → next**:

| Phase | Allowed | Forbidden |
|-------|---------|-----------|
| **Red** | Failing test(s) only that pin the step contract | Production edits “to see if it works” |
| **Green** | **Smallest surgical** production change to pass those tests | Drive-by refactors, dual paths, redesign in the same breath |
| **Refactor** | Under **all** relevant tests still green: dedupe, shared helper, right module, delete temporary one-offs | Weakening tests; starting the next step’s feature |
| **Then** | Move to the next step only after green + light refactor for this step | Bundling step N+1 into N’s green |

Steps that pin the invalid-heavy case specifically:

| Step | Contract slice |
|------|----------------|
| 2 | Selector skips invalid-marked candidates |
| 3 | Mid-branch connect fail restores pre-attempt tip |
| 7 | Heavy invalid M leaves tip on L; alternate valid heavier N wins if present |

Full plan lives in the agent session `plan.md` (steps 0–10).