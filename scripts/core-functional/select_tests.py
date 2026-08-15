#!/usr/bin/env python3
"""Build the Core functional run list from inventory.toml.

Used by run.sh. No node. Prints run names, skip names, or validates
requested names (must be inventory `run`).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    print("select_tests: need Python 3.11+ (tomllib)", file=sys.stderr)
    sys.exit(2)

HERE = Path(__file__).resolve().parent
DEFAULT_INVENTORY = HERE / "inventory.toml"


def load_rows(inventory_path: Path) -> list[dict]:
    data = tomllib.loads(inventory_path.read_bytes().decode())
    rows = data.get("test")
    if not isinstance(rows, list):
        raise SystemExit("inventory: missing [[test]] array")
    return [r for r in rows if isinstance(r, dict) and r.get("name")]


def classify(rows: list[dict]) -> tuple[list[str], list[str], dict[str, str]]:
    run: list[str] = []
    skip: list[str] = []
    status: dict[str, str] = {}
    for row in rows:
        name = str(row["name"])
        st = str(row.get("status", ""))
        status[name] = st
        if st == "run":
            run.append(name)
        elif st == "skip":
            skip.append(name)
    return run, skip, status


def normalize(name: str) -> str:
    base = Path(name).name
    return base if base.endswith(".py") else f"{base}.py"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    p.add_argument("--print-run", action="store_true")
    p.add_argument("--print-skip", action="store_true")
    p.add_argument(
        "--require-run",
        nargs="*",
        default=None,
        metavar="TEST",
        help="exit 0 iff every TEST is inventory status=run",
    )
    args = p.parse_args(argv)

    if not args.inventory.is_file():
        print(f"missing inventory: {args.inventory}", file=sys.stderr)
        return 1

    run, skip, status = classify(load_rows(args.inventory))

    if args.require_run is not None:
        for raw in args.require_run:
            name = normalize(raw)
            st = status.get(name)
            if st is None:
                print(f"unknown test: {name}", file=sys.stderr)
                return 1
            if st != "run":
                print(f"not in run set: {name}", file=sys.stderr)
                return 1
        return 0

    if args.print_run:
        for n in run:
            print(n)
    if args.print_skip:
        for n in skip:
            print(n)
    if not args.print_run and not args.print_skip:
        print("select_tests: pass --print-run, --print-skip, or --require-run", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
