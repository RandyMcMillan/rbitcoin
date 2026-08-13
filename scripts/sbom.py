#!/usr/bin/env python3
"""Emit CycloneDX 1.5 JSON from Cargo.lock (Q-21). No extra Rust tools.

Usage:
  python3 scripts/sbom.py > rbitcoin.cdx.json
  python3 scripts/sbom.py --out rbitcoin.cdx.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def parse_lock(text: str) -> list[dict]:
    pkgs: list[dict] = []
    cur: dict | None = None
    in_pkg = False
    for raw in text.splitlines():
        line = raw.rstrip()
        if line == "[[package]]":
            if cur and cur.get("name"):
                pkgs.append(cur)
            cur = {}
            in_pkg = True
            continue
        if not in_pkg or cur is None:
            continue
        if not line:
            continue
        if line.startswith("["):
            if cur.get("name"):
                pkgs.append(cur)
            cur = None
            in_pkg = False
            continue
        if " = " not in line:
            continue
        k, v = line.split(" = ", 1)
        v = v.strip().strip('"')
        if k in ("name", "version", "source", "checksum"):
            cur[k] = v
    if cur and cur.get("name"):
        pkgs.append(cur)
    return pkgs


def to_cyclonedx(pkgs: list[dict], root_name: str, root_ver: str) -> dict:
    comps = []
    seen: set[tuple[str, str]] = set()
    for p in pkgs:
        name = p.get("name") or ""
        ver = p.get("version") or ""
        if not name or not ver:
            continue
        key = (name, ver)
        if key in seen:
            continue
        seen.add(key)
        comp: dict = {
            "type": "library",
            "name": name,
            "version": ver,
            "purl": f"pkg:cargo/{name}@{ver}",
        }
        chk = p.get("checksum")
        if chk:
            comp["hashes"] = [{"alg": "SHA-256", "content": chk}]
        src = p.get("source")
        if src:
            comp["properties"] = [{"name": "cargo:source", "value": src}]
        comps.append(comp)
    comps.sort(key=lambda c: (c["name"].lower(), c["version"]))
    return {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "version": 1,
        "metadata": {
            "component": {
                "type": "application",
                "name": root_name,
                "version": root_ver,
            }
        },
        "components": comps,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--lock",
        type=Path,
        default=Path("Cargo.lock"),
        help="path to Cargo.lock",
    )
    ap.add_argument("--out", type=Path, default=None, help="write file (default stdout)")
    args = ap.parse_args()
    text = args.lock.read_text(encoding="utf-8")
    pkgs = parse_lock(text)
    bom = to_cyclonedx(pkgs, "rbitcoin", "0.1.0")
    blob = json.dumps(bom, indent=2, sort_keys=False) + "\n"
    if args.out:
        args.out.write_text(blob, encoding="utf-8")
    else:
        sys.stdout.write(blob)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
