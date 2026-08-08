# rbitcoin reports a store "corrupt record" for a peer-supplied invalid block

**Component:** `rbitcoin-query` (archive batch planner), surfaced via `rbitcoin-net`
**Commit:** `1ec7e42` (2026-08-07)
**Severity:** low — error misclassification; **not** a consensus divergence, no persistent
damage observed
**Relationship to 001:** same trigger — an invalid block arriving over BIP-152 compact-block
relay. This document covers only the *misclassification*; the peer disconnect it causes is
[001](./001-disconnect-on-invalid-block.md).
**Found by:** fuzzamoto (single-node rbitcoin campaign, then the Core-vs-rbitcoin
differential campaign)

## Summary

A block supplied by a peer can drive rbitcoin's archive batch planner into
`StoreError::Corrupt`, reported as:

```
consensus: store: corrupt record: archive: parent create_fk unresolved (contiguous batch required)
```

The node then terminates the peer session (see
[001](./001-disconnect-on-invalid-block.md), which is the impactful half of this).

The error text says "corrupt record", but nothing is corrupt: the input is simply an
invalid block. A remotely-triggerable corruption message makes genuine store corruption
indistinguishable from ordinary garbage input in logs and monitoring, and
`rbitcoin-net/src/ibd/events.rs:592` classifies this condition as **permanent** in the IBD
path, which is a poor fit for "a peer sent nonsense".

## Same delivery path as 001

The triggering block reaches rbitcoin exactly the way 001 describes — Bitcoin Core relays it
by compact block *before* validating it, and rbitcoin completes the reconstruction itself:

```
Saw new header hash=5325749f… height=201 peer=5
[validation] NewPoWValidBlock: block hash=5325749f…
[net] PeerManager::NewPoWValidBlock sending header-and-ids 5325749f… to peer=3
[net] PeerManager::NewPoWValidBlock sending header-and-ids 5325749f… to peer=7
[net] PeerManager::NewPoWValidBlock sending header-and-ids 5325749f… to peer=8   <- rbitcoin
[cmpctblock] peer=8 sent us a GETBLOCKTXN for block 5325749f…, sending a BLOCKTXN with 26 txns
```

rbitcoin could not fill the compact block from its mempool, requested the 26 missing
transactions with `getblocktxn`, reassembled the block, validated it, hit the store error,
and terminated the session. So this is not a separate netsplit vector — it is the same one,
reached through a different internal error path.

The error is also reachable without Core in the picture: in the single-node rbitcoin
campaign, blocks delivered directly as `block` messages produced it in 2 of 60 replayed
corpus entries. The *misclassification* is therefore independent of compact-block relay,
even though the netsplit consequence is shared with 001.

## It is not a consensus divergence

Explicitly checked, because the differential campaign initially flagged these as consensus
failures. They are not — the harness was failing to reconnect after rbitcoin dropped the
link, and once that was fixed both nodes agree:

```
Core:     UpdateTip: new best=6bdc5e59884d0b1dc6b22726e3fd76ee51bb1732266e17fc75d72addad70421c height=201
rbitcoin: UpdateTip: new best=6bdc5e59884d0b1dc6b22726e3fd76ee51bb1732266e17fc75d72addad70421c height=201
Test case ran successfully!
```

Same hash, same height. Furthermore the block that triggers the error is one **Core also
rejects**:

```
[validation] BlockChecked: block hash=5325749f… state=bad-txns-inputs-missingorspent,
             CheckTxInputs: inputs missing/spent in transaction 8f4d8a78…
InvalidChainFound: invalid block=5325749f… height=201
```

So both implementations reject the same block; they only differ in how the rejection is
classified and in rbitcoin dropping the peer over it.

There is also no evidence of lasting damage: after the error, rbitcoin went on to accept
the next block and reach the correct tip, and "corrupt record" appears exactly once in the
run.

## Mechanism

`archive_plan_batch_from` requires every non-coinbase input's parent transaction to be
resolvable either within the batch being planned or already in the store
(`crates/rbitcoin-query/src/archive.rs:455-463`):

```rust
if inp.create_fk.is_null() {
    if let Some(&cfk) = batch_map.get(&inp.prev_txid) {
        inp.create_fk = cfk;
    } else if let Some(&cfk) = resolved.get(&inp.prev_txid) {
        inp.create_fk = cfk;
    } else {
        return Err(StoreError::Corrupt(
            "archive: parent create_fk unresolved (contiguous batch required)",
        ));
    }
}
```

A block whose transactions spend outputs that do not exist reaches this planner and trips
the invariant. The condition is a real invariant for internally-generated batches, but it
is reachable from untrusted input, where "the peer sent a block spending nothing" is the
expected explanation rather than corruption.

Observed triggers, all peer-supplied blocks: transactions spending missing/spent inputs
(Core's verdict: `bad-txns-inputs-missingorspent`), and blocks containing a duplicated
transaction.

## Suggested direction

Distinguish "this batch is inconsistent because the store is damaged" from "this batch is
inconsistent because the block is invalid". The planner is called on both trusted
(internally generated) and untrusted (peer-supplied) paths; only the former warrants
`StoreError::Corrupt`. On the peer path this should surface as an ordinary consensus
rejection — and per [001](./001-disconnect-on-invalid-block.md) it should not take the
connection down.

## Affected testcases

Six of the nine testcases in `testcases/` trigger this variant:
`timeout-02c9fabf1a8b93fa`, `timeout-1f2ffe523b16e63a`, `timeout-412e22c01c515b95`,
`timeout-58f9b13bc2f81bfd`, `timeout-5b20e38b9f8d82d0`, `timeout-6ce7a951b12459e2`.

It also occurs against rbitcoin alone, without Bitcoin Core in the picture — 2 of 60
replayed corpus entries from the single-node campaign ended sessions this way.
