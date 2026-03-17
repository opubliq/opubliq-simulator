#!/usr/bin/env bash
# Push embeddings for all questions missing one.
# Idempotent: skips questions that already have an embedding.
#
# Usage:
#   ./ingestion/push_embeddings.sh               # all surveys
#   ./ingestion/push_embeddings.sh eeq_2022      # single survey
#   ./ingestion/push_embeddings.sh --dry-run     # dry run, no writes

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO_ROOT/venv/bin/python"

SURVEY=""
EXTRA_FLAGS=""

for arg in "$@"; do
  case "$arg" in
    --dry-run) EXTRA_FLAGS="--dry-run" ;;
    *)         SURVEY="$arg" ;;
  esac
done

SURVEY_FLAG=""
if [ -n "$SURVEY" ]; then
  SURVEY_FLAG="--survey $SURVEY"
fi

echo "==> Pushing embeddings$([ -n "$SURVEY" ] && echo " for $SURVEY" || echo " for all surveys")..."
"$VENV" "$REPO_ROOT/ingestion/generate_embeddings.py" $SURVEY_FLAG $EXTRA_FLAGS
