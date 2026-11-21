#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

chmod +x "$ROOT_DIR/tools/pre_commit_check.sh" "$ROOT_DIR/.githooks/pre-commit"
git config core.hooksPath .githooks
cp "$ROOT_DIR/.githooks/pre-commit" "$ROOT_DIR/.git/hooks/pre-commit"
chmod +x "$ROOT_DIR/.git/hooks/pre-commit"

echo "Installed MiniOB pre-commit hook (core.hooksPath=.githooks and .git/hooks/pre-commit)."
