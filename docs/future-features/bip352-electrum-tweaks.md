# BIP-352 Electrum tweaks (`blockchain.tweaks.subscribe`)

**Status:** implementing on `feat/electrum-tweaks-naive` — **naive on-the-fly**, no
index, no cache, no confirm hook, no `SCHEMA` change.

Cake-compatible Silent Payments **tweak server** (client holds scan keys; server
never sees `scan_sk`). Not Frigate’s Remote Scanner.

## Product

Cake Wallet asks the selected Electrum node whether it can serve BIP-352 tweaks.
`getNodeIsElectrs()` requires `server.version[0]` (lowercased) to contain
`electrs`; only then does it call `getTweaks(0)` (`blockchain.tweaks.subscribe
[0, 1, false]`). If that gate fails, or the method is missing / errors, it
treats the node as non-SP and the scan isolate uses `electrs.cakewallet.com:50001`.

We implement `blockchain.tweaks.subscribe`, advertise `rbitcoin-electrs <ver>`
in `server.version` / `server.features.server_version`, and set
`silent_payments` / `tweaks` in features so a wallet pointed at rbitcoin is
marked tweak-capable.

## Architecture (this tree)

Query cannot depend on consensus (consensus already depends on query). The
engine and the Class A walk therefore live in **`rbitcoin-consensus`**, same
pattern as `accept_and_connect_block`.

```
Electrum dispatch
    → consensus::silent_payments::tweaks_for_height(query, params, h)
        → confirmed[h] → header_txs
        → packed bodies (get_tx_full)
        → parent outs via create_fk (get_meta_and_outputs)
        → tweak_from_tx(tx, prevouts)
    → Cake JSON (one JSON-RPC result)
```

| Crate | Role |
|-------|------|
| **`rbitcoin-consensus::silent_payments`** | Eligibility, extract `Ai`, `A_tweak = input_hash · ΣA`, Taproot outs. Official BIP-352 vectors. Store walk via `&Query`. |
| **`rbitcoin-electrum`** | Dispatch + Cake JSON + `count` cap + `server.features`. |
| **store / confirm / SCHEMA** | **Untouched.** |

Packed Class A does **not** store prev `scriptPubKey` or `prev_txid` on the
spender row. Parent type / P2TR x-only come from the create via `create_fk`;
identity for `outpointL` comes from `txid.body`. Do **not** `reconstruct_block`.

Pre-Taproot heights (`h < params.taproot_height()`): empty map, no body IO.
Mainnet 709632; regtest 0.

Electrum listen still requires `--shindex`. Tweaks do not read Class B.

### libsecp256k1 silent-payments module

libsecp256k1 0.6+ has `src/modules/silentpayments` (send + full-node scan).
This workspace pins **rust-secp256k1 0.29.1** via bitcoin 0.32.101;
**those Rust bindings do not expose the C module.**

We compute the **tweak-server** point with public APIs already in tree:

- `PublicKey::combine_keys` (Σ eligible `Ai`)
- tagged SHA256 `BIP0352/Inputs`
- `PublicKey::mul_tweak` (`input_hash · A`)

Eligibility / pubkey extract is Bitcoin script, not EC — it would stay in Rust
even after bindings land. The C recipient APIs take **scan_sk**; we never hold
that. When rust-secp256k1 grows a silent-payments feature we can swap the
sum/multiply internals only. Do not add a raw C FFI in this crate.

## Protocol (Cake / electrs-SP)

```text
blockchain.tweaks.subscribe
params: [height, count, historicalMode]
probe:  [0, 1, false]
```

| Param | v1 |
|-------|----|
| `height` | Inclusive start. |
| `count` | Requested heights; **hard cap 8**. |
| `historicalMode` | Accepted, ignored. |

**Result:** one JSON-RPC **result** object keyed by height string. Cake’s
`fromJson` wants that map (not `null`, not a JSON-RPC error).

```json
{
  "850000": {
    "<txid display hex>": {
      "tweak": "<33-byte compressed hex>",
      "output_pubkeys": {
        "<vout>": ["<32-byte x-only hex>", <value_sats>]
      }
    }
  }
}
```

Locked from live `electrs.cakewallet.com:50001` on 2026-08-12
(`[850000, 1, false]`):

- `tweak` is **33-byte compressed** (02/03…).
- `output_pubkeys[vout][0]` is **64-char x-only** (BIP341), not a 33-byte key.
- txids are Electrum **display-order** hex.

Cake electrs also **pushes notifications** (data, then `{"message":"done"}`) and
puts a wrapped `done` in the JSON-RPC result. It **ignores** `server.features`
(method hangs / empty). v1 still returns the **data map as the result** — that
is what `fromJson` accepts — and does not stream notifications.

Height 0 / empty block: `{"0": {}}`.

`[0, 1, false]` on Cake electrs currently dumps their first indexed height
(seen: **823807**) as a notification. We do **not** copy that; genesis is empty
and the probe only needs a parseable success.

### Advertise capability

| Cake | rbitcoin |
|------|----------|
| `server.version[0].toLowerCase().contains('electrs')` | `"rbitcoin-electrs <ver>"` (also in `server.features.server_version`) |
| Then call `blockchain.tweaks.subscribe` `[0, 1, false]` | Method exists; JSON-RPC **success** with a map |
| Error / timeout / unparseable / no `electrs` in version → no SP | Must not be `unknown method` |
| Features (unused today) | `"silent_payments": [0]`, `"tweaks": true` |

Current Cake `_handleScanSilentPayments` still hardcodes
`tcp://electrs.cakewallet.com:50001` (`shouldSwitchNodes` commented;
`ScanNode` unused). A passing probe sets `node.supportsSilentPayments` and
would pass the wallet URI into the isolate, but the isolate does not use it.
Advertising is still required so Cake even calls `getTweaks`; OPERATOR states
the isolate caveat.

## BIP-352 server tweak

Eligible tx (scanning rules):

1. At least one BIP341 Taproot output.
2. At least one input from P2TR / P2WPKH / P2SH-P2WPKH / P2PKH.
3. No input spends witness version **> 1** (skip the whole tx).
4. Skip NUMS-H taproot script-path internals; skip uncompressed keys; skip
   invalid P2SH (not P2SH-P2WPKH).
5. `A = Σ Ai` (even-Y lift for P2TR). Infinity → skip.
6. `outpointL` = lexicographically smallest **serialized** outpoint in the tx
   (all inputs; txid internal + vout LE).
7. `input_hash = hash_BIP0352/Inputs(outpointL || ser_P(A))`.
8. Serve `A_tweak = input_hash · A` (33-byte compressed).

## Why naive (no index)

Cake needs a working RPC, not a 10 GiB side file. Probe is one empty height.
Live scan of one height is one block. We **cap `count`** so a client cannot
walk 250 k blocks on one TCP call.

Measured on the **9p agent VM** (mainnet store tip 962200, 4210-tx block):

| Step | Wall |
|------|------|
| Sequential packed body | ~12 ms (1.38 MiB) |
| Random parent-out peek | ~0.43 ms |
| EC (`input_hash · ΣA`) | ~10–50 ms / typical tip block |

On-the-fly here is **~1.5–3 blocks/s** (parent-bound). Local SSD is faster; do
not treat 9p numbers as operator-host IBD.

| `count` | This VM (naive) |
|---------|-----------------|
| 1 (probe / tip) | ~0.1–0.7 s |
| 8 (cap) | ~3–8 s |
| 2016 (if allowed) | ~10–25 min — **do not** |

A later thin index (33 B tweak + taproot outs per eligible tx) is optional. At
~252 k post-Taproot blocks and a few hundred eligible txs/block, fat index is
low single-digit GiB; thin tweak-only is hundreds of MiB. **Out of scope for v1.**

## Non-goals (v1)

- `--sptweaks`, `sp_tweaks.*`, confirm write, reorg truncate
- Caching
- Frigate `scan_sk`, Blindbit REST, mempool tweaks
- Esplora `/tweaks/:height`

## Tests

| Gate | Command |
|------|---------|
| Engine | `cargo test -p rbitcoin-consensus silent_payments` |
| Store walk | `cargo test -p rbitcoin-consensus tweaks_for_height` |
| RPC / Cake advertise | `cargo test -p rbitcoin-electrum tweaks` |

Synthetic `/tmp` stores only. Do not open the mainnet datadir in default tests.

## Operator

See `OPERATOR.md` (Electrum). Naive, uncached, `count` cap 8, Cake probe
contract, scan-isolate caveat.
