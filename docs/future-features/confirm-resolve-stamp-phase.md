# Confirm: split lookup into resolve + stamp (shipped)

Shipped on the four-stage pipeline (no fifth OS thread). IBD **lookup** is the
BQ-ahead TipOnly `head_fk` wave; **load** claims resolve-complete heights and
stamps from BQ hits plus leftover TipOnly `tx.head` (open head / ages ≤3
sealed). Remaining externals after the wave are expected. One-shot
`confirm_wire_run` / `accept_branch` still stamps in-process with TipOnly.

See the lookup-BQ-ahead plan (session) and `confirm_bq_resolve_wave`.

## Related

- [Class A storage pack](./class-a-storage-pack.md) — disk size, not this rate work.
