# rbitcoin drops peers that relay an invalid block (netsplit risk)

**Component:** `rbitcoin-net` (P2P peer session)
**Commit:** `1ec7e42` (2026-08-07)
**Severity:** medium — remotely triggerable peer eviction; no crash, no consensus split
**Status:** fixed — `drain_pending` keeps the session on accept failure (no disconnect/score)
**Found by:** fuzzamoto differential campaign (Bitcoin Core primary vs rbitcoin reference,
`oracle_consensus`)

## Summary

rbitcoin terminates a peer session as soon as that peer delivers a block that fails
validation. Bitcoin Core legitimately forwards blocks it has **not yet validated** over
BIP-152 compact-block relay, so an honest Core peer will hand rbitcoin an invalid block
whenever anyone produces one with valid proof of work. rbitcoin responds by dropping that
peer.

A single PoW-valid-but-invalid block therefore causes every rbitcoin node to disconnect
every Core peer that relayed it — simultaneously, and at exactly the moment the network is
under stress. This has real precedent: the July 2015 BIP66 forks produced exactly this kind
of block on mainnet.

## Why Core's behaviour is legitimate

Core relays a compact block as soon as the header has valid PoW and extends the tip, before
full validation, to minimise block propagation latency. That is the point of BIP-152
high-bandwidth mode. From an observed run:

```
Saw new header hash=2cb1dff5… height=201 peer=5
[validation] NewPoWValidBlock: block hash=2cb1dff5…
[net] PeerManager::NewPoWValidBlock sending header-and-ids 2cb1dff5… to peer=3
[net] PeerManager::NewPoWValidBlock sending header-and-ids 2cb1dff5… to peer=7
[net] PeerManager::NewPoWValidBlock sending header-and-ids 2cb1dff5… to peer=8   <- rbitcoin
[validation] BlockChecked: state=bad-txns-inputs-missingorspent
InvalidChainFound: invalid block=2cb1dff5… height=201
[error] ConnectTip: ConnectBlock 2cb1dff5… failed, bad-txns-inputs-missingorspent
```

Core relays to three peers, *then* validates, rejects, and keeps its tip at height 200. It
does not disconnect anyone. rbitcoin, on the same block:

```
WARN p2p: session 127.0.0.1:55190 ended: consensus: bad block: duplicate txid
```

## Mechanism

Block bodies arriving on a session are queued and connected by `drain_pending`, which
returns the consensus error verbatim rather than scoring it
(`crates/rbitcoin-net/src/peer.rs:855-862`):

```rust
match hub.accept_block(block) {
    Ok(AcceptOutcome::Accepted { .. })
    | Ok(AcceptOutcome::AlreadyHave)
    | Ok(AcceptOutcome::IgnoredWeaker) => { progress = true; }
    Err(e) => return Err(e),
}
```

Both delivery paths propagate that error out of the frame handler with `?`:

* `NetworkMessage::Block` — `peer.rs:687-697`
* `NetworkMessage::CmpctBlock` — `peer.rs:698-705` (after `try_fill_cmpct` reconstructs)

and the session loop propagates it again (`peer.rs:374-389`):

```rust
handle_peer_frame(…).await?;
```

so the session future returns `Err` and the connection ends (`peer.rs:404-406`).

Note that this is not the ban-score path. rbitcoin has a graduated misbehaviour score with
a disconnect threshold of 100 (`peer.rs:41`, `BAN_SCORE_THRESHOLD`) used for lesser offences
— oversize frames, rate limits, unsolicited `blocktxn`, bad `getblocktxn` indices. An
invalid block bypasses it and kills the session on the first occurrence; the check at
`peer.rs:389` is only reached when `handle_peer_frame` returns `Ok`, which an invalid block
never does. Routing it into the score instead would still be wrong (see below) — the point
here is only that the disconnect is immediate and unconditional.

## Impact

* Any peer relaying a PoW-valid invalid block is dropped immediately, with no scoring and
  no distinction between "peer forwarded someone else's invalid block" and "peer authored
  it". Under BIP-152 the forwarding peer is usually honest.
* The eviction is correlated across the network: every rbitcoin node drops its Core peers
  at the same time, on the same block.
* Mitigating factor: there is **no persistent banlist** in the tree (`peer_dos.rs` states
  it is explicitly not Core banlist parity), and `ban_score` is per-session state, so the
  address is not blocked and a fresh connection is accepted. The partition lasts until
  peers are re-established rather than being permanent.
* Producing a PoW-valid invalid block costs real hashpower on mainnet, so this is a
  miner-capable or fork-event scenario, not a cheap remote attack. On regtest/signet it is
  free.

## Suggested direction

A peer must **not** be disconnected *or* scored for forwarding an invalid block that
arrived via BIP-152. Under high-bandwidth relay the forwarder has not validated the block
either — that is the entire point of the mechanism — so an invalid block says nothing about
the sender's honesty. Scoring rather than disconnecting would only delay the same outcome:
repeated fork events would accumulate score against honest peers until they are dropped
anyway.

Bitcoin Core codifies exactly this. `MaybePunishNodeForBlock` takes a `via_compact_block`
flag whose documented purpose is to suppress punishment
(`src/net_processing.cpp:654-660`):

```
 * @param[in] via_compact_block this bool is passed in because net_processing should
 * punish peers differently depending on whether the data was provided in a compact
 * block message or not. If the compact block had a valid header, but contained invalid
 * txs, the peer should not be punished. See BIP 152.
```

The fix in rbitcoin is therefore to make the `accept_block` failure non-fatal to the
session on the compact-block path (`peer.rs:698-705`): drop the block, keep the connection.
Distinguishing paths matters — a block delivered as a full `block` message in response to
our own `getdata` carries the same argument (we asked for it), whereas a peer that *authored*
an invalid block and announced it by header is a different case. When in doubt, the safe
default for a block relay path is to reject the block and keep the peer.

## Secondary observation: differing rejection reason

Core and rbitcoin reject the same block for different reasons:

| node | verdict |
| :-- | :-- |
| Bitcoin Core | `bad-txns-inputs-missingorspent`, `CheckTxInputs: inputs missing/spent in transaction c0bf4d3e…` |
| rbitcoin | `bad block: duplicate txid` |

Both reject, so this is not a consensus split. But rbitcoin received the block via
`try_fill_cmpct` reconstruction (`peer.rs:703`), so it is worth confirming whether it
validated the block Core actually sent, or a mis-assembled one — a reconstruction that
duplicated a transaction would explain "duplicate txid" and would be a separate bug. Not
established either way here.
