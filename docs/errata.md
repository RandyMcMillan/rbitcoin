# Errata

Known limitations that are **not** treated as current must-fix bugs.
Durable confirm still uses TipOnly (connected instance). See
[`invariants.md`](./invariants.md) leftover identity and
[`017-duplicate-txid-unconnected-instance.md`](./external_findings/017-duplicate-txid-unconnected-instance.md).

## RAM leftover maps: one fk per txid

In-flight `creates` and pipeline pin `by_txid` are `txid → one fk` (last
write / newest layer). A second `create_fk` for the same txid **clobbers**
the first in that map. Do not stall the pipeline on a second fk (forget
cannot run).

**Pre-BIP30:** clobber is correct enough. The maps are a thin write-behind
/ pipeline identity, not a BIP30 instance list. Newest/last entry is an
acceptable single identity; durable TipOnly still prefers connected.

**Post-BIP30:** a second *connected* create of the same txid is invalid.
The only realistic overlap is a **disconnected** Class A sibling (reorg,
same tx on the new tip) still sitting in a RAM map while the new fk is
noted. Last-write could hide one of them — a *possible* identity
visibility hole. Unlikely: both rows stay on disk; TipOnly still picks
connected; n−1 is held in in-flight until after the child pin.

Do not grow these maps to `Vec<Fk>` unless a mainnet miss is shown to
be this case.
