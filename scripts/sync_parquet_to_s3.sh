#!/usr/bin/env bash
# sync_parquet_to_s3.sh
# Daily sync of all WNBA parquet files to S3.
# Only uploads new or changed files (aws s3 sync compares ETags).
# Usage: S3_BUCKET=<bucket-name> ./scripts/sync_parquet_to_s3.sh

set -euo pipefail

if [[ -z "${S3_BUCKET:-}" ]]; then
  echo "Error: S3_BUCKET environment variable is required."
  exit 1
fi

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

echo "Syncing parquet files to S3..."
echo "  From: $REPO_ROOT/wnba/"
echo "  To:   s3://$S3_BUCKET/wnba/"
echo ""

aws s3 sync "$REPO_ROOT/wnba/" "s3://$S3_BUCKET/wnba/" \
  --exclude "*" \
  --include "*/parquet/*.parquet" \
  --storage-class INTELLIGENT_TIERING

echo "Sync complete."
