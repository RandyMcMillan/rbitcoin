#!/usr/bin/env python3
"""Warn when a newer Bitcoin Core *release* exists than our inventory pin.

Nightly (and humans) should bump `third_party/bitcoin` and refresh fixtures +
`inventory.toml` when this warns. Default exit is 0 (warn only) so a stale pin
does not red the job. `--fail-on-stale` is opt-in.

Compares semver of non-draft, non-prerelease GitHub releases — not
`/releases/latest`, because Core still ships older-line maintenance tags
after a newer major.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

try:
    import tomllib
except ImportError:  # pragma: no cover
    print("check_core_release: need Python 3.11+ (tomllib)", file=sys.stderr)
    sys.exit(2)

HERE = Path(__file__).resolve().parent
DEFAULT_INVENTORY = HERE / "inventory.toml"
RELEASES_URL = "https://api.github.com/repos/bitcoin/bitcoin/releases?per_page=100"
UA = "rbitcoin-core-functional-release-check"


def parse_core_version(tag: str) -> tuple[int, int, int] | None:
    """v31.1 / 31.1 / v32.0.1 → (31,1,0). Reject rc/alpha/beta."""
    s = tag.strip()
    if s.lower().startswith("v"):
        s = s[1:]
    low = s.lower()
    if any(tok in low for tok in ("rc", "alpha", "beta", "pre")):
        return None
    parts = s.split(".")
    if len(parts) < 2:
        return None
    try:
        major = int(parts[0])
        minor = int(parts[1])
        patch = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        return None
    if major < 0 or minor < 0 or patch < 0:
        return None
    return (major, minor, patch)


def format_ver(v: tuple[int, int, int]) -> str:
    if v[2]:
        return f"v{v[0]}.{v[1]}.{v[2]}"
    return f"v{v[0]}.{v[1]}"


def pin_from_inventory(path: Path) -> str:
    data = tomllib.loads(path.read_bytes().decode())
    pin = data.get("pin")
    if not pin or not isinstance(pin, str):
        raise SystemExit(f"inventory missing pin: {path}")
    return pin.strip()


def versions_from_releases(rows: list[object]) -> list[tuple[str, tuple[int, int, int]]]:
    out: list[tuple[str, tuple[int, int, int]]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("draft") or row.get("prerelease"):
            continue
        tag = row.get("tag_name")
        if not isinstance(tag, str):
            continue
        ver = parse_core_version(tag)
        if ver is None:
            continue
        out.append((tag if tag.startswith("v") else f"v{tag}", ver))
    return out


def fetch_releases() -> list[object]:
    req = urllib.request.Request(RELEASES_URL, headers={"User-Agent": UA, "Accept": "application/vnd.github+json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def warn(msg: str) -> None:
    print(msg, file=sys.stderr)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        # Strip newlines so the annotation stays one line.
        safe = msg.replace("\n", " ").replace("\r", " ")
        print(f"::warning title=Bitcoin Core pin stale::{safe}", file=sys.stderr)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--inventory", type=Path, default=DEFAULT_INVENTORY)
    p.add_argument("--pin", default=None, help="override inventory pin (e.g. v31.1)")
    p.add_argument("--latest", default=None, help="treat this tag as the newest final release")
    p.add_argument(
        "--releases-json",
        type=Path,
        default=None,
        help="GitHub /releases JSON fixture (no network)",
    )
    p.add_argument(
        "--fail-on-stale",
        action="store_true",
        help="exit 1 when a newer final release exists (nightly stays warn-only)",
    )
    args = p.parse_args(argv)

    if args.pin:
        pin_s = args.pin
    else:
        if not args.inventory.is_file():
            print(f"missing inventory: {args.inventory}", file=sys.stderr)
            return 1
        pin_s = pin_from_inventory(args.inventory)
    pin = parse_core_version(pin_s)
    if pin is None:
        print(f"check_core_release: unparseable pin {pin_s!r}", file=sys.stderr)
        return 1
    pin_label = pin_s if pin_s.startswith("v") else f"v{pin_s}"

    if args.latest:
        latest_ver = parse_core_version(args.latest)
        if latest_ver is None:
            print(f"check_core_release: unparseable --latest {args.latest!r}", file=sys.stderr)
            return 1
        latest_label = args.latest if args.latest.startswith("v") else f"v{args.latest}"
    else:
        try:
            if args.releases_json:
                rows = json.loads(args.releases_json.read_text())
            else:
                rows = fetch_releases()
        except (OSError, urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
            warn(f"check_core_release: could not list Core releases: {e}")
            return 0
        if not isinstance(rows, list):
            warn("check_core_release: releases payload is not a list")
            return 0
        parsed = versions_from_releases(rows)
        if not parsed:
            warn("check_core_release: no final Core releases in payload")
            return 0
        latest_label, latest_ver = max(parsed, key=lambda t: t[1])

    if latest_ver > pin:
        warn(
            f"check_core_release: WARNING pin={pin_label} latest={latest_label} — "
            f"bump third_party/bitcoin and refresh fixtures + functional inventory"
        )
        return 1 if args.fail_on_stale else 0

    print(f"check_core_release: ok pin={pin_label} latest={format_ver(latest_ver)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
