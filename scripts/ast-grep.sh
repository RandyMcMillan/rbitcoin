#!/usr/bin/env bash
# Structural scan (Q-51). Error-severity findings fail the job.
# Prefer the `ast-grep` binary; Linux `sg` is setgroups.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if ! command -v ast-grep >/dev/null 2>&1; then
  echo "ast-grep not found; install via nix-shell / nix develop or CI taiki-e/install-action" >&2
  exit 1
fi

exec ast-grep scan --error crates
