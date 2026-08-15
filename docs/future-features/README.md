# Potential future features

Design notes and implementation plans that are **not** active workstreams.
They capture research and wire/protocol locks so we can pick them up later
without re-deriving client compatibility from scratch.

| Note | Status |
|------|--------|
| [Class A storage pack](./class-a-storage-pack.md) | **Landing on schema 17** (PR 45): spent 8 B, thin meta, script kinds 0–9 including P2A. Inwit Δfk still parked (cold). |
| [Confirm resolve + stamp phase](./confirm-resolve-stamp-phase.md) | **Shipped** (#29/#33). BQ-ahead TipOnly lookup; load stamps leftover TipOnly. |

BIP-352 Electrum tweaks shipped (`--sptweaks`, `OPERATOR.md`, `COMPAT.md`).

When a plan becomes active work, move or link it from the main docs / PR
description and update this index.
