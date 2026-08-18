#!/usr/bin/env bash
# Default crate graph: product + default-suite tests only.
# No host-forensics examples, cargo benches, or store_bench bin.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

PASS=0
FAIL=0

ok() { echo "ok - $1"; PASS=$((PASS + 1)); }
bad() { echo "not ok - $1"; FAIL=$((FAIL + 1)); }

# --- Cargo.toml: no [[bench]], no store_bench bin ---
if grep -R --include='Cargo.toml' -n '^\[\[bench\]\]' crates; then
  bad "Cargo.toml still lists [[bench]]"
else
  ok "no [[bench]] in crates/*/Cargo.toml"
fi

if grep -R --include='Cargo.toml' -n 'rbitcoin-store-bench' crates; then
  bad "Cargo.toml still lists rbitcoin-store-bench"
else
  ok "no rbitcoin-store-bench bin"
fi

# --- Auto-discovered examples ---
EXAMPLES=$(find crates -path '*/examples/*.rs' -print)
if [ -n "$EXAMPLES" ]; then
  echo "$EXAMPLES"
  bad "crate examples/ still present"
else
  ok "no crates/*/examples/*.rs"
fi

# --- cargo metadata (honest compile-graph pin) ---
if command -v cargo >/dev/null 2>&1; then
  META=$(cargo metadata --no-deps --format-version 1 --offline 2>/dev/null \
    || cargo metadata --no-deps --format-version 1)
  KIND_HITS=$(printf '%s' "$META" | python3 -c '
import json, sys
meta = json.load(sys.stdin)
bad = []
for pkg in meta.get("packages", []):
    if "rbitcoin" not in pkg.get("name", ""):
        continue
    for t in pkg.get("targets", []):
        kinds = t.get("kind") or []
        name = t.get("name", "")
        if "bench" in kinds:
            bad.append(f"{pkg[\"name\"]} bench {name}")
        if "example" in kinds and (
            name.startswith("diag_") or name == "dump_wit"
        ):
            bad.append(f"{pkg[\"name\"]} example {name}")
        if "bin" in kinds and name == "rbitcoin-store-bench":
            bad.append(f"{pkg[\"name\"]} bin {name}")
if bad:
    print("\n".join(bad))
')
  if [ -n "$KIND_HITS" ]; then
    echo "$KIND_HITS"
    bad "cargo metadata still lists toy targets"
  else
    ok "cargo metadata has no toy benches/examples/store_bench"
  fi
else
  echo "skip - cargo not on PATH (toml/file checks still apply)"
fi

# --- Leftover files cargo metadata does not see ---
for f in \
  crates/rbitcoin-net/tests/freeze_benches.rs \
  crates/rbitcoin-net/tests/reader_contention.rs \
  crates/rbitcoin-consensus/tests/diag_tip961461.rs \
  crates/rbitcoin-store/src/bin/store_bench.rs \
  crates/rbitcoin-store/testdata/head_resolve_cand_fks.sample.json \
  crates/rbitcoin-store/testdata/README-cand-fk-fixture.md
do
  if [ -e "$ROOT/$f" ]; then
    bad "leftover $f"
  else
    ok "gone $f"
  fi
done

if grep -n 'fn id_stage_page_group_microbench' \
    crates/rbitcoin-store/src/txid_body.rs >/dev/null; then
  bad "txid_body still has id_stage_page_group_microbench"
else
  ok "no id_stage_page_group_microbench"
fi

if grep -n 'fn microbench_get_many_wall_vs_serial_seed' \
    crates/rbitcoin-store/src/scripthash_head.rs >/dev/null; then
  bad "scripthash_head still has microbench_get_many_wall_vs_serial_seed"
else
  ok "no microbench_get_many_wall_vs_serial_seed"
fi

echo
echo "$PASS passed, $FAIL failed"
test "$FAIL" -eq 0
