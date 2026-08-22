#!/usr/bin/env bash
# Tag and push vX.Y.Z so GitHub Actions release.yml builds operator snapshots.
#
# Typical: merge the version-bump PR into master locally, then:
#   ./scripts/release.sh              # check, annotated tag, push master + tag
#   ./scripts/release.sh --dry-run    # checks only
#   ./scripts/release.sh --no-push    # tag locally, do not push
#
# Version is workspace.package.version (Cargo.toml). Files that must match:
# Cargo.toml, nix/rbitcoin.nix, CHANGELOG.md ## [X.Y.Z]. Tag is vX.Y.Z.
# Pushes the current branch (master) and the tag. Does not force-push or
# rewrite remotes.
set -euo pipefail

ROOT=""
DRY=0
PUSH=1
ALLOW_BRANCH=""
REMOTE="origin"

usage() {
  echo "usage: $0 [--dry-run] [--no-push] [--allow-branch NAME] [--remote NAME] [--root DIR]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY=1 ;;
    --no-push) PUSH=0 ;;
    --allow-branch)
      [[ $# -ge 2 ]] || usage
      ALLOW_BRANCH="$2"
      shift
      ;;
    --remote)
      [[ $# -ge 2 ]] || usage
      REMOTE="$2"
      shift
      ;;
    --root)
      [[ $# -ge 2 ]] || usage
      ROOT="$2"
      shift
      ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
  shift
done

if [[ -z "$ROOT" ]]; then
  ROOT="$(cd "$(dirname "$0")/.." && pwd)"
fi
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }

cargo_workspace_version() {
  awk '
    /^\[workspace.package\]/ { p = 1; next }
    p && /^\[/ { exit }
    p && /^version = "/ {
      gsub(/"/, "", $3)
      print $3
      exit
    }
  ' "$ROOT/Cargo.toml"
}

nix_package_version() {
  awk '
    /^[[:space:]]*version = "/ {
      gsub(/[";]/, "", $3)
      print $3
      exit
    }
  ' "$ROOT/nix/rbitcoin.nix"
}

changelog_has_heading() {
  local ver="$1"
  grep -qE "^## \\[${ver}\\]" "$ROOT/CHANGELOG.md"
}

changelog_notes() {
  local ver="$1"
  awk -v ver="$ver" '
    $0 ~ ("^## \\[" ver "\\]") { p = 1; next }
    p && /^## \[/ { exit }
    p { print }
  ' "$ROOT/CHANGELOG.md" | sed -e :a -e '/^\n*$/{$d;N;ba' -e '}'
}

ver="$(cargo_workspace_version)"
[[ "$ver" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "Cargo.toml workspace version is not X.Y.Z: ${ver:-empty}"
tag="v${ver}"

nix_ver="$(nix_package_version)"
[[ "$nix_ver" == "$ver" ]] || die "nix/rbitcoin.nix version=$nix_ver != Cargo.toml $ver"

changelog_has_heading "$ver" || die "CHANGELOG.md has no ## [$ver] heading"

notes="$(changelog_notes "$ver")"
[[ -n "$(printf '%s\n' "$notes" | grep -v '^[[:space:]]*$')" ]] || die "CHANGELOG.md ## [$ver] section is empty"

if [[ -n "$(git status --porcelain)" ]]; then
  die "working tree is not clean"
fi

branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ -n "$ALLOW_BRANCH" ]]; then
  [[ "$branch" == "$ALLOW_BRANCH" ]] || die "on $branch, expected --allow-branch $ALLOW_BRANCH"
elif [[ "$branch" != "master" && "$branch" != "main" ]]; then
  die "on $branch; merge first or pass --allow-branch $branch"
fi

if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
  die "local tag $tag already exists"
fi
if git remote get-url "$REMOTE" >/dev/null 2>&1; then
  if git ls-remote --exit-code "$REMOTE" "refs/tags/${tag}" >/dev/null 2>&1; then
    die "remote $REMOTE already has $tag"
  fi
elif [[ "$DRY" -eq 0 && "$PUSH" -eq 1 ]]; then
  die "no git remote $REMOTE"
fi

echo "release: version=$ver tag=$tag branch=$branch dry=$DRY push=$PUSH"
echo "---- CHANGELOG $ver ----"
echo "$notes"
echo "------------------------"

if [[ "$DRY" -eq 1 ]]; then
  echo "release: dry-run ok (no tag)"
  exit 0
fi

msg="rbitcoin ${tag}

${notes}"
git tag -a "$tag" -m "$msg"
echo "release: created annotated $tag at $(git rev-parse --short HEAD)"

if [[ "$PUSH" -eq 0 ]]; then
  echo "release: not pushed (--no-push). Push with:"
  echo "  git push ${REMOTE} refs/heads/${branch} refs/tags/${tag}"
  exit 0
fi

git push "$REMOTE" "refs/heads/${branch}" "refs/tags/${tag}"
echo "release: pushed ${branch} + ${tag} → $REMOTE"
echo "release: GitHub Actions .github/workflows/release.yml builds musl/Windows/Darwin"
echo "release: https://github.com/reardencode/rbitcoin/releases/tag/${tag}"
