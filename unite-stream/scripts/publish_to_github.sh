#!/usr/bin/env bash
# Create github.com/luoluoyuyu/unite_stream and push main.
# Requires: gh auth login  (or GH_TOKEN with repo scope)
set -euo pipefail

REPO="luoluoyuyu/unite_stream"
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "error: not a git repository: $ROOT" >&2
  exit 1
fi

if gh auth status -h github.com >/dev/null 2>&1; then
  echo "Creating $REPO via gh and pushing..."
  gh repo create "$REPO" --public --source=. --remote=origin --push \
    --description "Parser/runtime separation for Daft (requires daft==0.3.0.dev0+unite)"
  echo "Done: https://github.com/$REPO"
  exit 0
fi

if [[ -n "${GH_TOKEN:-}" ]]; then
  echo "Creating $REPO via API..."
  curl -fsS -X POST -H "Authorization: Bearer ${GH_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    https://api.github.com/user/repos \
    -d "{\"name\":\"unite_stream\",\"description\":\"Parser/runtime for Daft\",\"private\":false}"
  git remote remove origin 2>/dev/null || true
  git remote add origin "git@github.com:${REPO}.git"
  git push -u origin main
  echo "Done: https://github.com/$REPO"
  exit 0
fi

cat >&2 <<'EOF'
Cannot create GitHub repo automatically (gh not logged in, GH_TOKEN unset).

Option A — one-time gh login, then re-run:
  gh auth login -h github.com -p ssh -s repo
  ./scripts/publish_to_github.sh

Option B — create empty repo in browser, then push:
  1. https://github.com/new  → name: unite_stream  → Create (no README)
  2. git remote add origin git@github.com:luoluoyuyu/unite_stream.git   # if needed
  3. git push -u origin main

Interim: code is on branch unite_stream of luoluoyuyu/Daft (if push succeeded).
EOF
exit 1
