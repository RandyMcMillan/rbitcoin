# Head-resolve cand→fk fixture (dev microbench only)

## Purpose

Dev A/B for page-grouped `txid.body` identity (one fetch per wave) vs serial peeks.
**Not** a CI gate. Agent VM must not open production mainnet datadir.

## Checked-in sample

`head_resolve_cand_fks.sample.json` — synthetic **hot-locality** cand lists
(clustered create_fks near the sidefile tip). Shape:

```json
{
  "schema": 1,
  "note": "synthetic clustered cands for page-group ID microbench",
  "keys": [
    { "cands": [1001, 1002, 900], "want": 1001 },
    ...
  ]
}
```

- `cands`: ordered create_fk candidates (deepest / BIP30-first first)
- `want`: optional expected winner fk (for golden checks when body is built to match)

## Host capture recipe (real stamp)

1. On operator host with a live store, instrument end of `id_idx_wave_*` input:
   dump `cands_u64` per key for one stamp batch (10k–50k keys).
2. Write JSON with the same shape (omit full chain; fk lists only).
3. Run microbench with:
   `RBITCOIN_CAND_FK_FIXTURE=/path/to/dump.json cargo test -p rbitcoin-store --lib id_stage_page_group_microbench -- --ignored --nocapture`

Synthetic sample is enough for agent go/no-go when host dump is unavailable.
