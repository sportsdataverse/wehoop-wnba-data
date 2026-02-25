#!/usr/bin/env bash
# sync_parquet_to_s3.sh
# Daily sync of all WNBA parquet files to S3.
# Only uploads new or changed files (aws s3 sync compares ETags).
# Usage: ./scripts/sync_parquet_to_s3.sh <bucket-name>

set -euo pipefail

BUCKET="${1:-}"

if [[ -z "$BUCKET" ]]; then
  echo "Usage: $0 <bucket-name>"
  echo "Example: $0 wehoop-wnba-data"
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Syncing parquet files to S3..."
echo "  From: $REPO_ROOT/wnba/"
echo "  To:   s3://$BUCKET/wnba/"

echo ""

aws s3 sync "$REPO_ROOT/wnba/" "s3://$BUCKET/wnba/" \
  --exclude "*" \
  --include "*/parquet/*.parquet" \
  --storage-class INTELLIGENT_TIERING

echo "Sync complete."
